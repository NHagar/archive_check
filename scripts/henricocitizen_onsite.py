# https://www.henricocitizen.com/2020/11/02/?post_type=oht_article
import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

from tqdm import tqdm

con = sqlite3.connect("./data/henricocitizen.db")

results = []
years = [2010, 2015, 2020]
days = range(1, 31)

for y in years:
    print(f"Starting November {y}")

    for d in tqdm(days):
        url = f"https://www.henricocitizen.com/{y}/11/{str(d).zfill(2)}/?post_type=oht_article"
        r = requests.get(url)
        soup = BeautifulSoup(r.content)
        links = [i["href"] for i in soup.find_all("a", {"class": "oht-title-link"})]
        results.extend(links)

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")