import pathlib
import pickle
import sqlite3

import pandas as pd

from pipeline import db, analysis

DATA_PATH = pathlib.Path("./data")
RESULTS_PATH = pathlib.Path("./results")
databases = list(DATA_PATH.glob("*.db"))

# TODO: This should be more modular via CLI
for d in databases:
    # Load and set up file structure
    dpath = RESULTS_PATH / d.name.replace(".db", "")
    dpath.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(d)
    data = db.Database(con)
    # Join and preprocess
    # Get list of table names to compare
    tables = [i for i in data.list_tables() if i!="parsed_articles"]
    # Get article data for each table, filter to desired timeframe
    tables_cleaned = [(t, data.join_to_parsed_clean(t, "2020-11-01", "2020-11-30")) for t in tables]
    # Instantiate class for each table
    tables_cleaned = [analysis.Table(*t) for t in tables_cleaned]
    # URL counts
    url_counts = [{"service": i.name, "urls": i.count_urls()} for i in tables_cleaned]
    url_counts = pd.DataFrame(url_counts)
    url_counts.to_csv(dpath / "urlcounts.csv", index=False)
    # LDA
    # NLP preprocessing
    # TODO: This process produces terrible LDA models - 
    # probably a consequence of minimal preprocessing.
    # We should convert this to an sklearn pipeline, and adhere to
    # the best practices suggested in this paper -
    # https://www.tandfonline.com/doi/abs/10.1080/19312458.2018.1430754?journalCode=hcms20
    nlp = analysis.init_spacy(stopwords=["advertisement", "Advertisement", "said", "Said"],
                              disabled=['tok2vec', 'tagger', 'parser', 'ner', 'attribute_ruler', 'lemmatizer'])
    for t in tables_cleaned:
        t.process_body(nlp)
        t.build_corpus()
        t.train_models(20)
    best_models = [t.get_best_model() for t in tables_cleaned]
    best_models = pd.DataFrame(best_models)
    best_models.to_csv(dpath / "lda.csv", index=False)
    # Headline analysis
    # TODO: convergence warning
    nlp = analysis.init_spacy()
    for t in tables_cleaned:
        t.process_hed(nlp)
        model = t.logistic_regression()
        # TODO: figure out how to format and present these models
        with open(dpath / "model.pickle", "wb") as f:
            pickle.dump(model, f)
