import pathlib
import re
import sqlite3
from time import sleep

import newspaper
import pandas as pd
from tqdm import tqdm

from pipeline.db import Database

# Site-specific patterns to weed out non-story URLs
PATTERNS = {
    "journalgazette": re.compile("202011[0-9]{2}"),
    "latimes": re.compile("2020-11-[0-9]{2}"),
    "vox": re.compile("2020\/11\/[0-9]{1,2}")
}

data_path = pathlib.Path("./data")

dbs = sorted(list(data_path.glob("*.db")))

for d in dbs:
    # Set up database connection
    con = sqlite3.connect(d)
    db = Database(con)
    # Load and clean URLs
    sitename = d.name.replace(".db", "")
    p = PATTERNS[sitename]
    urls = db.get_url_superset()
    print(f"Raw url count: {len(urls)}")
    cleaned_urls = db.clean_urls(urls, p)
    print(f"Cleaned url count: {len(cleaned_urls)}")
    # Compare to already-parsed list
    parsed_urls = db.get_urls_from_table("parsed_articles")
    toparse = cleaned_urls - parsed_urls   
    # Download loop
    for i in tqdm(toparse):
        ntk = newspaper.Article(i)
        ntk.download()
        try:
            ntk.parse()

            result = {
                "url": i,
                "url_canonical": ntk.canonical_link,
                "text": ntk.text,
                "hed": ntk.title,
                "pub_date": ntk.publish_date
            }
            rdf = pd.DataFrame([result])
            db.save_table(rdf, "parsed_articles", append=True)

        except newspaper.ArticleException as e:
            s = str(e)
            if "404" in s:
                print("404")
            else:
                print("other error")
        sleep(1)