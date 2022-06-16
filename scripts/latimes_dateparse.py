import asyncio
import sqlite3

from playwright.async_api import async_playwright

from tqdm import tqdm

from pipeline.db import Database

con = sqlite3.connect("./data/latimes.db")
db = Database(con)
parsed = db.read_table("parsed_articles")
onsite = db.get_urls_from_table("onsite")
# check if pub date is none
nopub = parsed[(parsed.pub_date.isnull()) & (parsed.url.isin(onsite))]

pub_dates = []

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        for _,v in tqdm(nopub.iterrows()):
            u = v.url_canonical
            await page.goto(u)
            handle = await page.query_selector("time")
            if handle is not None:
                pub_dt = await handle.get_attribute("datetime")
            else:
                pub_dt = ""
            pub_dates.append(pub_dt)

asyncio.run(main())

nopub.pub_date = pub_dates
nopub.drop(columns="index").to_sql("parsed_articles", con, if_exists="append")