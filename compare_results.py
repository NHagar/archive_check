import pathlib
import sqlite3

import pandas as pd

from pipeline import db, analysis

DATA_PATH = pathlib.Path("./data")
RESULTS_PATH = pathlib.Path("./results")
databases = list(DATA_PATH.glob("*.db"))

for d in databases:
    # Load and set up file structure
    dpath = RESULTS_PATH / d
    dpath.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(d)
    data = db.Database(con)
    # Join and preprocess
    tables = [i for i in data.list_tables() if i!="parsed_articles"]
    tables_cleaned = [(t, data.join_to_parsed_clean(t, "2020-11-01", "2020-11-30")) for t in tables]
    tables_cleaned = [analysis.Table(*t) for t in tables_cleaned]
    # URL counts
    url_counts = [{"service": i.name, "urls": i.count_urls()} for i in tables_cleaned]
    url_counts = pd.DataFrame(url_counts)
    url_counts.to_csv(dpath / "urlcounts.csv", index=False)
    # LDA
    # NLP preprocessing
    nlp = analysis.init_spacy(["advertisement", "Advertisement", "said", "Said"],
                              ['tok2vec', 'tagger', 'parser', 'ner', 'attribute_ruler', 'lemmatizer'])
    for t in tables_cleaned:
        t.process_body(nlp)
        t.build_corpus()
        t.train_models(20)
    best_models = [t.get_best_model() for t in tables_cleaned]
    best_models = pd.DataFrame(best_models)
    best_models.to_csv(dpath / "lda.csv", index=False)
    # Headline analysis
