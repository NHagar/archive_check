# %%
import datetime
import xml.etree.ElementTree as ET

import requests

# %%
years = [2020]
for y in years:
    start = datetime.datetime(y, 11, 1).timestamp()
    end = datetime.datetime(y, 11, 2).timestamp()
    url = f"https://news.vice.com/en/sitemap/articles?locale=en_us&before={end}&after={start}"
    r = requests.get(url)

# %%
tree = ET.fromstring(r.content)
# %%
tree.findall("url")
# %%
urls = [i for i in tree.iter() if "url" in i.tag and "set" not in i.tag]
# %%
len(urls)
# %%
