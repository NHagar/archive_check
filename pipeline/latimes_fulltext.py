# %%
import pathlib
import sqlite3
from time import sleep

import newspaper
import pandas as pd

from tqdm import tqdm

# %%
data = pathlib.Path("./data")
alldf = pd.read_csv( data / "lat_all_urls.csv")
con = sqlite3.connect("./data/lat.db")

# %%
novdf = alldf[(alldf['url'].str.contains("/story/")) &
              (alldf['url'].str.contains("2020-11"))]
# %%
urls = list(set(novdf['url']))

c = con.cursor()
c.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='parsed_articles' ''')
if c.fetchone()[0]:
    parsed_urls = pd.read_sql_query("SELECT * FROM parsed_articles", con)['url'].tolist()
    urls = [i for i in urls if i not in parsed_urls]
# %%
len(urls)

# %%
for i in tqdm(urls):
    ntk = newspaper.Article(i)
    ntk.download()
    try:
        ntk.parse()

        result = {
            "url": i,
            "url_canonical": ntk.canonical_link,
            "text": ntk.text,
            "hed": ntk.title,
            "pub_date": ntk.publish_date
        }
        rdf = pd.DataFrame([result])
        rdf.to_sql("parsed_articles", con, if_exists="append")

    except newspaper.ArticleException as e:
        s = str(e)
        if "404" in s:
            print("404")
        else:
            print("other error")
    sleep(1)
# %%
