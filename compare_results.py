import pathlib
import sqlite3

from pipeline import db, analysis

DATA_PATH = pathlib.Path("./data")
RESULTS_PATH = pathlib.Path("./results")
databases = list(DATA_PATH.glob("*.db"))

for d in databases:
    # Load and preprocess
    dpath = RESULTS_PATH / d
    dpath.mkdir(parents=True, exist_ok=True)
    data = db.Database(d)
    tables = [i for i in data.list_tables() if i!="parsed_articles"]
    tables_cleaned = [data.join_to_parsed(t) for t in tables]
    tables_cleaned = [analysis.clean_dataframe(t) for t in tables_cleaned]
    
    # Clean tables
    for t in tables:
        df = data.join_to_parsed(t)
        df_cleaned = analysis.clean_dataframe(df)
        # URL count
        urls = len(df_cleaned.url_canonical.unique())
        

# LDA


# Headline analysis
