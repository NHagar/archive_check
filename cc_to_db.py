# %%
import json
import pathlib
import sqlite3

import pandas as pd

from pipeline import db

# %%
domains = [("journalgazette.db", "www.journalgazette.net"), 
           ("latimes.db", "www.latimes.com"), 
           ("vox.db", "www.vox.com")]

ccdirs = list(pathlib.Path("./data/cc_download_articles_v2").iterdir())

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
