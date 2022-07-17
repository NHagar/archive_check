# Primary source
import asyncio
import pathlib
import sqlite3

import pandas as pd
from playwright.async_api import async_playwright

con = sqlite3.connect("./data/latimes.db")

years = [2010, 2015, 2020]

index_pages = [f"https://www.latimes.com/sitemap/{y}/11" for y in years]

async def main():
    archived_urls = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        for index_page in index_pages:
            p = 1
            while True:
                await page.goto(f"{index_page}/p/{p}")
                if "/p/" in page.url:
                    links = await page.evaluate('''() => {
                        var main = document.querySelector("ul.archive-page-menu");
                        var links = Array.prototype.slice.call(main.querySelectorAll("a.link"));
                        var urls = links.map(function(elem){ return elem.getAttribute("href") });

                        return urls
                    }''')
                    archived_urls.extend(links)
                    p += 1
                else:
                    break
        await browser.close()
    archived_series = pd.Series(archived_urls, name="url")
    archived_series.to_sql("onsite", con, if_exists="replace")

asyncio.get_event_loop().run_until_complete(main())