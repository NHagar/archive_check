# %%
import pathlib
import random
import string
import sqlite3

from joblib import Parallel, delayed
from octis.models.LDA import LDA
from octis.dataset.dataset import Dataset
from octis.evaluation_metrics.diversity_metrics import TopicDiversity
from octis.evaluation_metrics.coherence_metrics import Coherence
import pandas as pd
import spacy

from tqdm import tqdm

# %%
con = sqlite3.connect("./data/lat.db")
nlp = spacy.load("en_core_web_sm", disable=['tok2vec', 'tagger', 'parser', 'ner', 'attribute_ruler', 'lemmatizer'])
# Add "advertisement" and "said" to our stopwords list
nlp.vocab["advertisement"].is_stop = True
nlp.vocab["said"].is_stop = True
nlp.vocab["Advertisement"].is_stop = True
nlp.vocab["Said"].is_stop = True
# %%
df = pd.read_sql_query("SELECT * FROM parsed_articles", con)

# %%
services = pd.read_csv("./data/lat_all_urls.csv")
novdf = services[(services['url'].str.contains("/story/")) &
              (services['url'].str.contains("2020-11"))]

# %%
adf = novdf.merge(df, how="left", on="url")


# %%
def process_document(doc):
    lemma_list = [str(tok.text).lower() for tok in doc
                  if tok.is_alpha and not tok.is_stop]
    lemmas = " ".join(lemma_list) 
    return lemmas


def process_chunk(texts):
    preproc_pipe = []
    for doc in tqdm(nlp.pipe(texts, batch_size=20)):
        preproc_pipe.append(process_document(doc))
    return preproc_pipe


# %%
# Per-service count
counts_raw = novdf.groupby("service").count()['url']

# %%
# Per-service 404s
missing = adf[pd.isna(adf['url_canonical'])].groupby("service").count()['url']

# %%
# 404-adjusted counts
counts_raw - missing

# %%
adf = adf[~pd.isna(adf['text'])]

# %%
# LDA preprocessing
adf['preproc'] = process_chunk(adf['text'])

# %%
sname = "gdelt"
test = adf[adf['service']==sname]

# %%
# Corpus setup
def build_corpus(df, sname):
    corpus_path = pathlib.Path(f"./data/corpus_{sname}")
    corpus_path.mkdir(parents=True, exist_ok=True)
    with open(corpus_path / "corpus.tsv", "w", encoding="utf-8") as f:
        f.write("\n".join(df['preproc'].tolist()))
    with open(corpus_path / "corpus.txt", "w", encoding="utf-8") as f:
        tokens = df['preproc'].tolist()
        wordlist = [i.split(" ") for i in tokens]
        wordlist = [i for l in wordlist for i in l]
        wordset = list(set(wordlist))
        f.write("\n".join(wordset))
    
    return corpus_path

def train_models(dataset, max_k):
    # Find optimal topics
    metrics = []
    for k in tqdm(range(3, max_k+1)):
        model = LDA(num_topics=k, alpha=0.1)
        output = model.train_model(dataset)

        npmi = Coherence(texts=dataset.get_corpus(), topk=10, measure='c_npmi')
        topic_diversity = TopicDiversity(topk=10)
        topic_diversity_score = topic_diversity.score(output)
        npmi_score = npmi.score(output)

        metrics.append({
            "k": k,
            "diversity": topic_diversity_score,
            "coherence": npmi_score
        })
    
    return metrics
# %%
random.seed(20210423)

services = ['gdelt', 'wayback', 'cc', 'mediacloud', 'onsite']
all_results = {}
for s in services:
    sdf = adf[adf['service']==s]
    corpus_path = build_corpus(sdf, s)
    dataset = Dataset()
    dataset.load_custom_dataset_from_folder(str(corpus_path))

    results = train_models(dataset, 20)

    all_results[s] = results
# %%
max_per_service = []
for k, v in all_results.items():
    max_result = sorted(v, key=lambda k_: k_['coherence'])[-1]
    max_formatted = {'service': k, "k": max_result['k'], "coherence": max_result['coherence']}
    max_per_service.append(max_formatted)
# %%
max_per_service
# %%
