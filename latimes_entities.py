# %%
import collections
import itertools
import sqlite3

from fuzzywuzzy import fuzz
import pandas as pd
from tqdm import tqdm
# %%
con = sqlite3.connect("./data/lat.db")
df = pd.read_sql_query("SELECT * FROM entities", con)
services = pd.read_csv("./data/lat_all_urls.csv")
novdf = services[(services['url'].str.contains("/story/")) &
            (services['url'].str.contains("2020-11"))]
meta = pd.read_sql_query("SELECT * FROM parsed_articles", con)[['url', 'url_canonical', 'pub_date']]
# %%
adf = df.merge(novdf)
adf = adf.merge(meta)
# %%
test = adf[adf['service']=="mediacloud"]
test.loc[:, "text"] = test['text'].apply(lambda x: x.replace("’s", "").replace("\n", ""))

# %%
test[(test['text'].str.contains("Biden")) & (test['type']=='PERSON')]

# %%

def add_item(item, match, overlap, lookup):
    if item in lookup:
        lookup[item].append((match, overlap))
    else:
        lookup[item] = [(match, overlap)]
    return lookup

# %%
records = list(test[test['type']=='PERSON'][['url_canonical', 'text']].to_records(index=False))
records = sorted(records, key=lambda x: x[1])
grouped = itertools.groupby(records, key=lambda x: x[1])
g_clean = []
for k,g in grouped:
    record = (k, {i[0] for i in g})
    g_clean.append(record)
g_clean = [i for i in g_clean if len(i[1])>1]
combos = itertools.combinations(g_clean, 2)
lookup_global = {}
for i in combos:
    i1 = i[0]
    i2 = i[1]
    # Check article set overlap
    overlap = len(i1[1].intersection(i2[1]))
    try:
        is_match = fuzz.partial_ratio(i1[0], i2[0])==100
    except ValueError:
        is_match = False
    if is_match:
        if len(i1[0])>len(i2[0]):
            add_item(i2[0], i1[0], overlap, lookup_global)
        else:
            add_item(i1[0], i2[0], overlap, lookup_global)
    else:
        pass

# %%
for i in lookup_global:
    lookup_global[i].sort(key=lambda x: x[1])
    

# %%
def build_lookup(combos):
    lookup = {}
    for c in combos:
        # Check for partial match
        try:
            is_match = fuzz.partial_ratio(c[0], c[1])==100
            # If partial match, add pair to lookup table
            if is_match:
                if len(c[0])>len(c[1]):
                    lookup[c[1]] = c[0]
                else:
                    lookup[c[0]] = c[1]                
            else:
                pass
        except ValueError:
            pass
    return lookup        


# %%
cleaned_ents = []
grouped = itertools.groupby(records, key=lambda x: x[0])
for k,g in grouped:
    # Clean and list out entities
    ents = [i[1] for i in g]
    ents = [i.replace("’s", "") for i in ents]
    ents_set = set(ents)
    # Build entity combinations for comparison
    combos = itertools.combinations(ents_set, 2)
    # Build lookup table for resolution
    lookup_table = build_lookup(combos)
    # Resolve entities
    resolved_ents = []
    for i in ents:
        # Check local lookup table first
        if i in lookup_table:
            resolved = lookup_table[i]
        # Then global lookup table, based on co-occurrence freq
        elif i in lookup_global:
            resolved = lookup_global[i][-1][0]
        # Then just use the ent, if no matches
        else:
            resolved = i
        resolved_ents.append(resolved)
    oriented = [{"url": k, "entity": i} for i in resolved_ents]
    cleaned_ents.extend(oriented)

# %%
df = pd.DataFrame(cleaned_ents)
# %%
g = df.groupby("entity").nunique().sort_values(by="url", ascending=False)
# %%
g[g['url']>1].head(20)
# %%
