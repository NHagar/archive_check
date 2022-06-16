# %%
import json
import pathlib
import sqlite3

import pandas as pd

from pipe import db

# %%
domains = [("gazette.db", "gazette.com"),
           ("jezebel.db", "jezebel.com"),
           ("nypost.db", "nypost.com"),
           ("buzzfeednews.db", "www.buzzfeednews.com"),
           ("journalgazette.db", "www.journalgazette.net"), 
           ("latimes.db", "www.latimes.com"), 
           ("mic.db", "www.mic.com"),
           ("nytimes.db", "www.nytimes.com"),
           ("politico.db", "www.politico.com"),
           ("usatoday.db", "www.usatoday.com"),
           ("vox.db", "www.vox.com"),
           ("wsj.db", "www.wsj.com")]

ccdirs = list(pathlib.Path("./data/cc_download_articles").iterdir())

# %%
for i in domains:
    con = sqlite3.connect(f"./data/{i[0]}")
    database = db.Database(con)
    site_dir = next(d for d in ccdirs if i[1] in str(d))
    files = list(site_dir.glob("*.json"))
    stories = []
    for f in files:
        with open(f, "r", encoding="utf-8") as file:
            data = json.load(file)
        stories.append({k: data[k] for k in ("date_publish", "url")})
    df = pd.DataFrame(stories)
    database.save_table(df, "commoncrawl")
# %%
