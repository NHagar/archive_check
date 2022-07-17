import sqlite3

from bs4 import BeautifulSoup
import pandas as pd
import requests

con = sqlite3.connect("./data/blockclubchicago.db")

years = [2020]
results = []

page = 338
while True:
    url = f"https://blockclubchicago.org/all/page/{page}/"
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    posts = soup.select("a[class*=default__LinkWrapper]")
    urls = [i["href"] for i in posts]
    filter_string = f"/{years[0]}/"

    if any(filter_string in i for i in urls):
        results.extend(urls)
        print(page)
        page += 1
    else:
        break


print(f"{len(results)} URLs found")

archive_series = pd.Series(results, name="url")
archive_series.to_sql("onsite", con)

print("URLs saved to database")