# %%
import sqlite3

import numpy as np
import pandas as pd
import scipy.stats
from tqdm import tqdm
# %%
con = sqlite3.connect("./data/lat.db")
df = pd.read_sql_query("SELECT * FROM entities", con)
services = pd.read_csv("./data/lat_all_urls.csv")
novdf = services[(services['url'].str.contains("/story/")) &
            (services['url'].str.contains("2020-11"))]
meta = pd.read_sql_query("SELECT * FROM parsed_articles", con)[['url', 'url_canonical', 'pub_date', 'text']]
# %%
df.loc[:, "ent_text"] = df['text'].apply(lambda x: x.replace("’s", "").replace("\n", ""))
df = df.drop("text", axis=1)
adf = df.merge(novdf, on="url")
adf = adf.merge(meta, on="url")

# %%
def get_article_lens(df):
    trump = df[(df['ent_text'].str.contains("Trump")) & 
                ~(df['ent_text'].str.contains("Jr"))]
    biden = df[(df['ent_text'].str.contains("Biden")) & 
                ~(df['ent_text'].str.contains("Hunter"))]

    trump_lens = [len(i) for i in set(trump['text'])]
    biden_lens = [len(i) for i in set(biden['text'])]
    
    return trump_lens, biden_lens

# %%
results = []
services = set(adf['service'])
for s in services:
    sdf = adf[adf['service']==s]
    trump, biden = get_article_lens(sdf)
    ttest = scipy.stats.ttest_ind(trump, biden)
    result = {"service": s, 
              "t_stat": ttest.statistic,
              "t_pval": ttest.pvalue,
              "biden_mean": np.mean(biden),
              "trump_mean": np.mean(trump)}
    results.append(result)

# %%
pd.DataFrame(results)

# %%
