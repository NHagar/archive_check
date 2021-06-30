import argparse
import pathlib
import pickle
import sqlite3

import pandas as pd

from pipeline import db, analysis

DATA_PATH = pathlib.Path("./data")
RESULTS_PATH = pathlib.Path("./results")
databases = list(DATA_PATH.glob("*.db"))

parser = argparse.ArgumentParser(description='Run set of analyses for collected data.')
parser.add_argument("--analyses", type=str, required=True, help="comma-separated list of analyses to run - count,lda,headlines")
parser.add_argument("--start", type=str, required=True, help="start date, of format Y-M-D")
parser.add_argument("--end", type=str, required=True, help="end date, of format Y-M-D")
args = parser.parse_args()

analyses = [i.strip() for i in args.analyses.split(",")]

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
    tables_cleaned = [(t, data.join_to_parsed_clean(t, args.start, args.end)) for t in tables]
    # Instantiate class for each table
    tables_cleaned = [analysis.Table(*t) for t in tables_cleaned]
    if "count" in analyses:
        # URL counts
        url_counts = [{"service": i.name, "urls": i.count_urls()} for i in tables_cleaned]
        url_counts = pd.DataFrame(url_counts)
        url_counts.to_csv(dpath / "urlcounts.csv", index=False)
    if "lda" in analyses:
        # LDA
        # NLP preprocessing
        nlp = analysis.init_spacy(disabled=['tok2vec', 'parser', 'ner'])
        for t in tables_cleaned:
            t.process_body(nlp)
            t.build_corpus()
            t.train_models(20)
        best_models = [t.get_best_model() for t in tables_cleaned]
        best_models = pd.DataFrame(best_models)
        best_models.to_csv(dpath / "lda.csv", index=False)
    if "headlines" in analyses:
        # Headline analysis
        # TODO: convergence warning
        nlp = analysis.init_spacy()
        for t in tables_cleaned:
            t.process_hed(nlp)
            model = t.logistic_regression()
            # TODO: figure out how to format and present these models
            with open(dpath / "model.pickle", "wb") as f:
                pickle.dump(model, f)
