import asyncio
import sqlite3

import pandas as pd
import pyppeteer

con = sqlite3.connect("./data/vox.db")

years = [2015, 2020]
index_pages = [f"https://www.vox.com/archives/{y}/11" for y in years]

BUTTON_SELECT = "button.c-archives-load-more__button"
results = []

async def autoScroll(page):
    await page.evaluate('''async () => {
        await new Promise((resolve, reject) => {
            var totalHeight = 0;
            var distance = 100;
            var timer = setInterval(() => {
                var scrollHeight = document.body.scrollHeight;
                window.scrollBy(0, distance);
                totalHeight += distance;

                if(totalHeight >= scrollHeight){
                    clearInterval(timer);
                    resolve();
                }
            }, 100);
        });
    }''')


async def main():
    browser = await pyppeteer.launch({"headless": False})
    page = await browser.newPage()
    await page.setViewport({"width": 900, "height": 1080})
    for index_page in index_pages:
        await page.goto(index_page)
        await page.waitFor(3000)
        await autoScroll(page)
        # Find and click "load more" until it disappears
        button = await page.querySelector(BUTTON_SELECT)
        while button:
            await page.focus(BUTTON_SELECT)
            await page.click(BUTTON_SELECT)
            await page.waitFor(5000)
            await autoScroll(page)
            button = await page.querySelector(BUTTON_SELECT)
        # Save all URLs on full page    
        urls = await page.evaluate('''() => {
                var links = document.querySelectorAll("h2.c-entry-box--compact__title > a");
                var links_array = [...links];
                var urls = links_array.map(function(elem) {return elem.getAttribute("href")});

                return urls;
            }
        ''')
        results.extend(urls)

    urls_series = pd.Series(results, name="url")
    urls_series.to_sql("onsite", con)

    await browser.close()

asyncio.get_event_loop().run_until_complete(main())