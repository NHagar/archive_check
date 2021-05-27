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
def pos_gen(df, pos):
    trump_urls = set(df[(df.hed.str.contains("Trump")) & ~(df.hed.str.contains("Jr"))].url)
    biden_urls = set(df[df.hed.str.contains("Biden")].url)
    both = trump_urls.intersection(biden_urls)
    trump_urls = trump_urls - both
    biden_urls = biden_urls - both
    pos_trump = pos[pos['url'].isin(trump_urls)]
    pos_biden = pos[pos['url'].isin(biden_urls)]

    return pos_trump, pos_biden

def reformat(df):
    df.loc[:, 'value'] = 1
    pv = df.pivot_table(index='url', columns='pos', values='value')
    return pv

# %%
results = {}
snames = set(services.service)
for s in snames:
    urls = novdf[novdf['service']==s].url
    subset = df[df['url'].isin(urls)]
    pos_trump, pos_biden = pos_gen(subset, pos)
    pos_trump = reformat(pos_trump)
    pos_biden = reformat(pos_biden)
    pos_trump.loc[:, 'candidate'] = 0
    pos_biden.loc[:, 'candidate'] = 1
    pos_all = pos_trump.append(pos_biden).fillna(0)
    pos_all = pos_all.sample(frac=1)

    X = pos_all.drop(columns=['candidate'])
    X = sm.add_constant(X)
    y = pos_all.candidate
    model = sm.Logit(y, X)
    fitted = model.fit(method="bfgs", maxiter=500)

    results[s] = fitted



# %%
results['gdelt'].summary()
# %%
