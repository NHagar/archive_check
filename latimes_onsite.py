# Primary source
import asyncio
import pathlib

import pyppeteer

index_page = "https://www.latimes.com/sitemap/2020/11"

async def main():
    archived_urls = []
    browser = await pyppeteer.launch({"headless": False})
    page = await browser.newPage()
    for i in range(1,12):
        await page.goto(f"{index_page}?p={i}")
        links = await page.evaluate('''() => {
            var main = document.querySelector("ul.archive-page-menu");
            var links = Array.prototype.slice.call(main.querySelectorAll("a.link"));
            var urls = links.map(function(elem){ return elem.getAttribute("href") });

            return urls
        }''')
        archived_urls.extend(links)
    await browser.close()
    output_path = pathlib.Path("./data/lat_links.csv")
    with open(output_path, "w", encoding="utf-8") as f:
        for i in archived_urls:
            f.write(f"{i}\n")

asyncio.get_event_loop().run_until_complete(main())