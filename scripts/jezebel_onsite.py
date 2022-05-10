import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/jezebel.db")

years = [2010, 2015, 2020]
results = []

for y in years:
    url = f"https://jezebel.com/sitemap/{y}/november"
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    urls = [i.find('a')['href'] for i in soup.find_all("h4")]
    results.extend(urls)

print(f"Found {len(results)} urls")

archived_series = pd.Series(results, name="url")
archived_series.to_sql("onsite", con)

print("URLs saved to database")