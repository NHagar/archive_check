# %%
import sqlite3

import pandas as pd

# %%
con = sqlite3.connect("./data/lat.db")
df = pd.read_sql_query("SELECT * FROM parsed_articles", con)
pos = pd.read_sql_query("SELECT * FROM partsofspeech", con)
services = pd.read_csv("./data/lat_all_urls.csv")
novdf = services[(services['url'].str.contains("/story/")) &
            (services['url'].str.contains("2020-11"))]
# %%
trump_urls = set(df[(df.hed.str.contains("Trump")) & ~(df.hed.str.contains("Jr"))].url)
biden_urls = set(df[df.hed.str.contains("Biden")].url)
both = trump_urls.intersection(biden_urls)
trump_urls = trump_urls - both
biden_urls = biden_urls - both
# %%
pos_trump = pos[pos['url'].isin(trump_urls)]
pos_biden = pos[pos['url'].isin(biden_urls)]
# %%

# %%
