import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

from tqdm import tqdm

con = sqlite3.connect("./data/nypost.db")

years = [2010, 2015, 2020]
results = []
for y in years:
    print(f"Starting November {y}")
    base_url = f"https://nypost.com/{y}/11/"
    for d in tqdm(range(1,31)):
        full_url = base_url + str(d).zfill(2)
        r = requests.get(full_url)
        soup = BeautifulSoup(r.text)
        links = [i.find("a")['href'] for i in soup.find_all("h3")]
        results.extend(links)

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")
