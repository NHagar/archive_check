import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect('./data/politico.db')

results = []
years = [2010, 2015, 2020]
url = "https://www.politico.com/politics/"
counter = 1

while True:
    full_url = url + str(counter)
    r = requests.get(full_url)
    soup = BeautifulSoup(r.text, "html.parser")
    days = soup.find_all("ul", {"class": "story-frag-list"})
    for d in days:
        dt = pd.to_datetime(d.find("time").get("datetime"))
        if dt.year in years and dt.month == 11:
            headlines = d.find_all("h3")
            links = [i.find("a")["href"] for i in headlines]
            results.extend(links)
        else:
            if dt.year < years[0]:
                break
            else:
                pass
    else:
        print(counter)
        counter += 1
        continue
    break

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")