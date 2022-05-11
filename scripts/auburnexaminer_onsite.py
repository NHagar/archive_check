import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/auburnexaminer.db")

years = [2020]
results = []

page = 1
while True:
    url = f"https://auburnexaminer.com/page/{page}/"
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    posts = soup.find_all("div", {"class": "post-header"})

    for i in posts:
        date = i.find("div", {"class": "post-byline"}).text.split(" on ")[-1]
        date = pd.to_datetime(date)
        if date.month == 11 and date.year in years:
            link = i.find("a")["href"]
            results.append(link)
        else:
            if date.year < years[0]:
                break
            else:
                pass
    else:
        print(page)
        page += 1
        continue
    break

print(f"{len(results)} URLs found")

archive_series = pd.Series(results, name="url")
archive_series.to_sql("onsite", con)

print("URLs saved to database")