# %%
import json
import pathlib

import pandas as pd
# %%
data = pathlib.Path("data")

# %%
def standardize(data, service):
    urls = list(set(data['url']))
    formatted = [{"url": i, "service": service} for i in urls]
    fdf = pd.DataFrame(formatted)

    return fdf

# %%
gdelt = pd.read_csv(data / "lat_gdelt.csv")
gdf = standardize(gdelt, "gdelt")
# %%
onsite = pd.read_csv(data / "lat_links.csv", header=None, names=['url'])
odf = standardize(onsite, "onsite")
# %%
mediacloud = pd.read_csv(data / "lat_mc.csv")
murls = list(set(mediacloud['guid']))
mformatted = [{"url": i, "service": "mediacloud"} for i in murls]
mdf = pd.DataFrame(mformatted)
# %%
wayback = pd.read_csv(data / "lat_wayback.csv")
wdf = standardize(wayback, "wayback")
# %%
def read_cc(fpath):
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)
    url = data['url']

    return url

# %%
cc_path = data / "cc_download_articles"
urls = [read_cc(i) for i in cc_path.glob("**/*") if str(i).endswith('json')]
# %%
cc_formatted = [{"url": i, "service": "cc"} for i in urls]
ccdf = pd.DataFrame(cc_formatted)
# %%
alldf = pd.concat([gdf, odf, mdf, wdf, ccdf])

# %%
alldf.loc[:, "url"] = alldf['url'].apply(lambda x: x.split("?")[0])
alldf = alldf.drop_duplicates(["url", "service"])
# %%
alldf.to_csv(data / "lat_all_urls.csv")
# %%
novdf = alldf[(alldf['url'].str.contains("/story/")) &
              (alldf['url'].str.contains("2020-11"))]
# %%
novdf.groupby("service").count()
# %%
