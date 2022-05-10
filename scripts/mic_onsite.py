import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/mic.db")

years = [2010, 2015, 2020]
sections = ['culture', 'innovation', 'wellbeing', 'current', 'identity', 'life', 'impact', 'info']
results = []

for y in years:
    for s in sections:
        url = f"https://www.mic.com/archive/november/{y}/{s}"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        urls = ["https://www.mic.com" + i.find("a")["href"] for i in soup.find_all("li")]
        results.extend(urls)

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")