import asyncio
import sqlite3

import pandas as pd
from playwright.async_api import async_playwright

from tqdm import tqdm

con = sqlite3.connect("./data/wallstreetjournal.db")

results = []

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        years = [2010, 2015, 2020]
        for y in years:
            print(f"Starting November {y}")
            base_url = f"https://www.wsj.com/news/archive/{y}/11/"
            for d in tqdm(range(1, 31)):
                full_url = f"{base_url}{str(d).zfill(2)}?page="
                counter = 1
                await page.goto(full_url + str(counter), wait_until="domcontentloaded")
                links = await page.eval_on_selector_all("h2 > a", "elements => elements.map(element => element.href)")
                results.extend(links)
                await page.wait_for_timeout(1000)
                while len(links)>0:
                    counter += 1
                    await page.goto(full_url + str(counter), wait_until="domcontentloaded")
                    links = await page.eval_on_selector_all("h2 > a", "elements => elements.map(element => element.href)")
                    results.extend(links)
                    await page.wait_for_timeout(1000)

        await browser.close()

asyncio.run(main())

print(f"Collected {len(results)} URLs")

# Store URLs to database
archived_series = pd.Series(results, name="url")
archived_series.to_sql("onsite", con)

print("URLs stored to database")