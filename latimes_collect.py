# %%
from datetime import datetime
import os

import asyncio
from bs4 import BeautifulSoup
import pyppeteer
import requests


from src import query_downloads

from dotenv import load_dotenv

load_dotenv()

# %%
domain = "latimes.com"
start_date = datetime.strptime("2020-11-01", "%Y-%m-%d")
end_date = datetime.strptime("2020-12-01", "%Y-%m-%d")
news_key = os.environ["API_KEY_NEWS"]
mc_key = os.environ["API_KEY_MC"]

# %%
# Internet archive
wayback = query_downloads.archive_query(domain, start_date, datetime.strptime("2021-04-19", "%Y-%m-%d"))

# %%
wayback.to_csv("./data/lat_wayback.csv")

# %%
# GDELT
gdelt = query_downloads.gdelt_query(domain, start_date, end_date)

# %%
gdelt.to_csv("./data/lat_gdelt.csv")

# %%
# Media Cloud
mediacloud = query_downloads.mediacloud_query(domain, start_date, end_date, mc_key)

# %%
mediacloud.to_csv("./data/lat_mc.csv")

# %%
