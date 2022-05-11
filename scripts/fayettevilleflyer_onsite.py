import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/fayettevilleflyer.db")

results = []
years = [2010, 2015, 2020]
page = 1 
while True:
    print(page)
    url = f"https://www.fayettevilleflyer.com/page/{page}/"
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    articles = soup.find_all("div", {"class": "entry-part"})
    for a in articles:
        date = a.find("li", {"class": "entry-date"})
        date = date.text.split("·")[-1].strip()
        date = pd.to_datetime(date)
        if date.month == 11 and date.year in years:
            link = a.find("h2", {"class": "entry-title"}).find("a")["href"]
            results.append(link)
        else:
            if date.year < years[0]:
                break
            else:
                pass
    else:
        page += 1
        continue
    break

print(f"{len(results)} URLs found")

results_series = pd.Series(results, name="url")
results_series.to_sql("onsite", con)

print("URLs saved to database")