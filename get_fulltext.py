import pathlib
import re
import sqlite3

import newspaper
from newsplease import NewsPlease
import pandas as pd
import requests
from tqdm import tqdm

from pipe.db import Database

# Site-specific patterns to weed out non-story URLs
PATTERNS = {
    "auburnexaminer": re.compile("^(?:(?!\?p=|\/category\/|\/tag\/|\/event\/|\/amp\/|\/wp-content\/).)+$"),
    "buzzfeednews": re.compile("\/article\/"),
    "fayettevilleflyer": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "gazette": re.compile("^(?:(?!\/amp|\.js|img\.|\/ap\/|\/search\/|\.amp|\/_services|\/register|\/feed\/|\/content\/|\/users\/|\/articles\/|\/tncms\/|\/mailto|\.jpg|\/classifieds\/|ww3\.|\.php|\/housing\/|\/multimedia\/|\/author\/|\/comments\/|\/db_images\/|checkout\.|\/common\/|\/events\/|\/blogs\/).)+$"),
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
        try:
            article = NewsPlease.from_url(i)
            r = requests.get(i)
            ad = article.get_serializable_dict()
            ad['url_canonical'] = r.url
            rdf = pd.DataFrame([ad])
            rdf.authors = rdf.authors.apply(lambda x: "__".join(x))
            db.save_table(rdf, "parsed_articles", append=True)
        except newspaper.article.ArticleException as e:
            db.save_table(pd.DataFrame([{"url": i, "error": "404"}]), "errors", append=True)