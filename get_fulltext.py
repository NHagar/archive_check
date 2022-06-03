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
    "auburnexaminer": re.compile("^(?:(?!\?p=|\/category\/|\/tag\/|\/event\/|\/amp\/|\/wp-content\/).)+$"),
    "buzzfeednews": re.compile("\/article\/"),
    "fayettevilleflyer": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "gazette": re.compile("\/article_"),
    "henricocitizen": re.compile("\/articles\/"),
    "jezebel": re.compile("^(?:(?!\/amp|\.js|img\.).)+$"),
    "journalgazette": re.compile("(2010|2015|2020)11[0-9]{2}"),
    "latimes": re.compile("(2010|2015|2020)-11-[0-9]{2}"),
    "mic": re.compile("^(?:(?!\.jpg|\.js).)+$"),
    "nypost": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "nytimes": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "politico": re.compile("(2020|2015|2010)\/11"),
    "usatoday": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "vox": re.compile("(2020|2015|2010)\/11\/[0-9]{1,2}"),
    "wsj": re.compile("\/articles\/")
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
    print(f"Raw url count for {d}: {len(urls)}")
    cleaned_urls = db.clean_urls(urls, p)
    print(f"Cleaned url count for {d}: {len(cleaned_urls)}")
    # Compare to already-parsed list
    parsed_urls = db.get_urls_from_table("parsed_articles")
    error_urls = db.get_urls_from_table("errors")
    toparse = cleaned_urls - parsed_urls   
    toparse = toparse - error_urls
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
                db.save_table(pd.DataFrame([{"url": i, "error": "404"}]), "errors", append=True)
            else:
                print("other error")
#        sleep(0.5)