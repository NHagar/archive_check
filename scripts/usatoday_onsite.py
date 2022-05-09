import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

from tqdm import tqdm

con = sqlite3.connect("./data/usatoday.db")

results = []
years = [2010, 2015, 2020]
for y in years:
    print(f"Starting November {y}")
    # build URL for each year
    base_url = f"https://www.usatoday.com/sitemap/{y}/november/"
    for d in tqdm(range(1, 31)):
        # Add in day
        full_url = f"{base_url}{d}/"
        counter = 1
        # Add in pagination
        final_url = f"{full_url}?page={counter}"
        r = requests.get(final_url)
        soup = BeautifulSoup(r.text)
        links = [i.find("a")["href"] for i in soup.find_all("li", {"class": "sitemap-list-item"}) if i.find("a")]
        links = [i for i in links if "/sitemap" not in i]
        results.extend(links)
        while len(links)>0:
            counter += 1
            final_url = f"{full_url}?page={counter}"
            r = requests.get(final_url)
            soup = BeautifulSoup(r.text)
            links = [i.find("a")["href"] for i in soup.find_all("li", {"class": "sitemap-list-item"}) if i.find("a")]
            links = [i for i in links if "/sitemap" not in i]
            results.extend(links)

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")