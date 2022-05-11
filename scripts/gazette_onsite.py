from random import betavariate
import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/gazette.db")

years = [2010, 2015, 2020]
results = []

for y in years:
    print(f"Starting November {y}")
    offset = 0
    while True:
        url = f"https://gazette.com/search/?d1={y}-11-01&d2={y}-11-30&s=start_time&sd=desc&l=100&t=article&nsa=eedition&o={offset}"
        r = requests.get(url)
        print(offset)
        soup = BeautifulSoup(r.content, "lxml")
        leads = soup.find_all("div", {"class": "card-lead"})
        if len(leads)>0:
            cards = [i.parent for i in leads]
            links = [i.find("a", {"class": "tnt-asset-link"})["href"] for i in cards]
            links = ["https://gazette.com" + i for i in links]
            results.extend(links)
            offset += 100
        else:
            break

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")