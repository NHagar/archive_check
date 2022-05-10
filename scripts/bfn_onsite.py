import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/buzzfeednews.db")

results = []

counter = 1
while True:
    url = f"https://www.buzzfeednews.com/sitemap/news/2020_{counter}.xml"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    for i in soup.find_all("url"):
        pub_date = pd.to_datetime(i.find("news:news").find("news:publication_date").text)
        if pub_date.month == 11:
            url = i.find("loc").text
            results.append(url)
    counter += 1
    if counter == 53:
        break

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")