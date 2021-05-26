# %%
import sqlite3

import pandas as pd
import statsmodels.api as sm

# %%
con = sqlite3.connect("./data/lat.db")
df = pd.read_sql_query("SELECT url, url_canonical, hed, pub_date FROM parsed_articles", con)
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
def reformat(df):
    df['value'] = 1
    pv = df.pivot_table(index='url', columns='pos', values='value')
    return pv

# %%
pos_trump = reformat(pos_trump)
pos_biden = reformat(pos_biden)
# %%
pos_trump.loc[:, 'candidate'] = 0
pos_biden.loc[:, 'candidate'] = 1
pos_all = pos_trump.append(pos_biden).fillna(0)

# %%
X = pos_all.drop(columns=['candidate']).to_numpy()
X = sm.add_constant(X)
y = pos_all.candidate.to_numpy()
# %%
model = sm.Logit(y, X)
# %%
result = model.fit_regularized()
# %%
result.summary()
# %%
