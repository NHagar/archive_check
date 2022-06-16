import argparse
import pathlib
import pickle
import sqlite3

import pandas as pd

from pipe import db, analysis

DATA_PATH = pathlib.Path("./data")
RESULTS_PATH = pathlib.Path("./results")
databases = list(DATA_PATH.glob("*.db"))

LDA_STOPWORDS = {
    "journalgazette": ["said", "people"],
    "latimes": ["said", "advertisement", "times", "people"],
    "vox": ["said", "people"]
}

parser = argparse.ArgumentParser(description='Run set of analyses for collected data.')
parser.add_argument("--analyses", type=str, required=True, help="comma-separated list of analyses to run - count,lda,headlines")
parser.add_argument("--start", type=str, required=True, help="start date, of format Y-M-D")
parser.add_argument("--end", type=str, required=True, help="end date, of format Y-M-D")
args = parser.parse_args()

analyses = [i.strip() for i in args.analyses.split(",")]

for d in databases:
    print(d)
    # Load and set up file structure
    sname = d.name.replace(".db", "")
    dpath = RESULTS_PATH / sname
    dpath.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(d)
    data = db.Database(con)
    # Join and preprocess
    # Get list of table names to compare
    tables = [i for i in data.list_tables() if i not in ["parsed_articles", "errors"]]
    # Get article data for each table, filter to desired timeframe
    tables_cleaned = [(t, data.join_to_parsed_clean(t, args.start, args.end)) for t in tables]
    tables_cleaned = [t for t in tables_cleaned if len(t[1]) > 0]
    # Instantiate class for each table
    tables_cleaned = [analysis.Table(*t) for t in tables_cleaned]
    if "count" in analyses:
        # URL counts
        url_counts = [{"service": i.name, "urls": i.count_urls()} for i in tables_cleaned]
        url_counts = pd.DataFrame(url_counts)
        url_counts.to_csv(dpath / f"urlcounts_{args.start}_{args.end}.csv", index=False)
    if "lda" in analyses:
        # LDA
        # NLP preprocessing
        try:
            stopwords = LDA_STOPWORDS[sname]
        except KeyError:
            stopwords = ["said", "people"]
        nlp = analysis.init_spacy(stopwords=stopwords, disabled=['tok2vec', 'parser', 'ner'])
        for t in tables_cleaned:
            t.process_body(nlp)
            t.build_corpus()
            t.train_models(40)
        best_models = [t.get_best_model() for t in tables_cleaned]
        best_models = pd.DataFrame(best_models)
        best_models.to_csv(dpath / f"lda_{args.start}_{args.end}.csv", index=False)
    if "headlines" in analyses:
        # Candidate headline analysis
        results = []
        for t in tables_cleaned:
            trump_count, biden_count = t.pres_headlines()
            r = {"service": t.name, "trump_count": trump_count, "biden_count": biden_count}
            results.append(r)
        pd.DataFrame(results).to_csv(dpath / f"headlinecounts_{args.start}_{args.end}.csv")