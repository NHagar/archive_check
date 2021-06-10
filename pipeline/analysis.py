from dataclasses import dataclass

import pandas as pd
import spacy
from tqdm import tqdm


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

    def _remove_stopwords(self, doc: spacy.tokens.Doc):
        """spacy processing to remove stopwords
        """
        lemma_list = [str(tok.text).lower() for tok in doc
                    if tok.is_alpha and not tok.is_stop]
        lemmas = " ".join(lemma_list) 
        return lemmas

    def count_urls(self) -> int:
        """Count unique URLs in dataframe
        """
        urls = len(self.df.url_canonical.unique())

        return urls

    def process_body(self, nlp: spacy.lang.en.English):
        """Pipeline to process body text with spacy
        """
        preproc_pipe = []
        for doc in tqdm(nlp.pipe(self.df.text, batch_size=20)):
            preproc_pipe.append(self._remove_stopwords(doc))
        self.df.loc[:, "body_parsed"] = preproc_pipe