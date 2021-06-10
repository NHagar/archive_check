import pathlib
import sqlite3

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
    tables = [i for i in data.list_tables() if i!="parsed_articles"]
    tables_cleaned = [(t, data.join_to_parsed(t)) for t in tables]
    tables_cleaned = [analysis.Table(*t) for t in tables_cleaned]


# LDA


# Headline analysis
