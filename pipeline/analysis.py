from dataclasses import dataclass
import pathlib
import random

from octis.models.LDA import LDA
from octis.dataset.dataset import Dataset
from octis.evaluation_metrics.diversity_metrics import TopicDiversity
from octis.evaluation_metrics.coherence_metrics import Coherence
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
import spacy
import statsmodels.api as sm
from tqdm import tqdm


random.seed(20210423)

def init_spacy(stopwords: list[str], disabled: list[str]) -> None:
    """Init spacy instance with desired parameters
    """
    nlp = spacy.load("en_core_web_lg", disable=disabled)
    for word in stopwords:
        nlp.vocab[word].is_stop = True

    return nlp

@dataclass
class Table:
    name: str
    df: pd.DataFrame

    def _remove_stopwords(self, doc: spacy.tokens.Doc) -> list[str]:
        """spacy processing to remove stopwords
        """
        lemmas = [str(tok.text).lower() for tok in doc
                    if tok.is_alpha and not tok.is_stop]
        return lemmas
    
    def _get_tags(self, doc: spacy.tokens.Doc) -> set[str]:
        """spacy processing to get POS tags
        """
        pos = set([tok.tag_ for tok in doc])

        return pos

    def _pres_headlines(self):
        """Grab parts of speech for relevant headline subset
        """
        trump_urls = set(self.df[(self.df.hed.str.contains("Trump")) & ~(self.df.hed.str.contains("Jr"))].url_canonical)
        biden_urls = set(self.df[self.df.hed.str.contains("Biden")].url_canonical)
        both = trump_urls.intersection(biden_urls)
        trump_urls = trump_urls - both
        biden_urls = biden_urls - both
        pos_trump = self.df[self.df.url_canonical.isin(trump_urls)].hed_pos
        pos_biden = self.df[self.df.url_canonical.isin(biden_urls)].hed_pos

        return pos_trump, pos_biden

    def _headlines_transform(self, headlines: pd.Series) -> pd.DataFrame:
        """One-hot encode headline parts of speech
        """
        mlb = MultiLabelBinarizer()
        df = pd.DataFrame(mlb.fit_transform(headlines),columns=mlb.classes_)
        return df

    def count_urls(self) -> int:
        """Count unique URLs in dataframe
        """
        urls = len(self.df.url_canonical.unique())

        return urls

    def process_body(self, nlp: spacy.Language) -> None:
        """Pipeline to process body text with spacy
        """
        preproc_pipe = []
        for doc in tqdm(nlp.pipe(self.df.text, batch_size=20)):
            preproc_pipe.append(self._remove_stopwords(doc))
        self.df.loc[:, "body_parsed"] = preproc_pipe
    
    def process_hed(self, nlp: spacy.Language) -> None:
        """Pipeline to process headline text with spacy
        """
        preproc_pipe = []
        for doc in tqdm(nlp.pipe(self.df.hed, batch_size=20)):
            preproc_pipe.append(self._get_tags(doc))
        self.df.loc[:, "hed_pos"] = preproc_pipe
        pos_trump, pos_biden = self._pres_headlines()
        self.pos_trump = self._headlines_transform(pos_trump)
        self.pos_biden = self._headlines_transform(pos_biden)

    
    def build_corpus(self) -> None:
        """Construct corpus for LDA
        """
        corpus_path = pathlib.Path(f"./data/corpus_{self.name}")
        corpus_path.mkdir(parents=True, exist_ok=True)
        with open(corpus_path / "corpus.tsv", "w", encoding="utf-8") as f:
            f.write("\n".join([" ".join(i) for i in self.df['body_parsed'].tolist()]))
        with open(corpus_path / "corpus.txt", "w", encoding="utf-8") as f:
            tokens = self.df['body_parsed'].tolist()
            wordlist = [i for l in tokens for i in l]
            wordset = list(set(wordlist))
            f.write("\n".join(wordset))
        
        dataset = Dataset()
        dataset.load_custom_dataset_from_folder(str(corpus_path))
        self.lda_dataset = dataset

    def train_models(self, max_k: int) -> list[dict]:
        """Train LDA models over a range of values
        """
        metrics = []
        for k in tqdm(range(3, max_k+1)):
            model = LDA(num_topics=k, alpha=0.1)
            output = model.train_model(self.lda_dataset)

            npmi = Coherence(texts=self.lda_dataset.get_corpus(), topk=10, measure='c_npmi')
            topic_diversity = TopicDiversity(topk=10)
            topic_diversity_score = topic_diversity.score(output)
            npmi_score = npmi.score(output)

            metrics.append({
                "k": k,
                "diversity": topic_diversity_score,
                "coherence": npmi_score
            })
        
        self.metrics = metrics

    def get_best_model(self) -> dict:
        """Select optimal model from trained range
        """
        best = sorted(self.metrics, key=lambda x: x['coherence'])[-1]
        best['service'] = self.name

        return best
    
    def logistic_regression(self):
        self.pos_trump.loc[:, 'candidate'] = 0
        self.pos_biden.loc[:, 'candidate'] = 1
        pos_all = self.pos_trump.append(self.pos_biden).fillna(0)
        pos_all = pos_all.sample(frac=1)
        X = pos_all.drop(columns=['candidate'])
        X = sm.add_constant(X)
        y = pos_all.candidate
        model = sm.Logit(y, X)
        fitted = model.fit(method="bfgs", maxiter=500)

        return fitted