import os
import sqlite3

import pandas as pd
import requests

from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

con = sqlite3.connect("./data/nytimes.db")

# API key
nyt_key = os.environ.get("NYT_KEY")

# Query endpoint for desired months
results = []

years = [2010, 2015, 2020]
for year in tqdm(years):
    url = f"https://api.nytimes.com/svc/archive/v1/{year}/11.json?api-key={nyt_key}"
    response = requests.get(url)
    data = response.json()['response']['docs']
    urls = [d['web_url'] for d in data]
    results.extend(urls)

print(f"{len(results)} URLs found")

# Store URLs to database
archived_series = pd.Series(results, name="url")
archived_series.to_sql("onsite", con)
print("URLs stored to database")