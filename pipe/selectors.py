import re
from typing import Dict

import pandas as pd
from playwright.sync_api import sync_playwright
from tqdm import tqdm

from pipe.db import Database

# Site-specific patterns to weed out non-story URLs
PATTERNS = {
    "auburnexaminer": re.compile("^(?:(?!\?p=|\/category\/|\/tag\/|\/event\/|\/amp\/|\/wp-content\/).)+$"),
    "blockclubchicago": re.compile("2020\/11\/[0-9]{2}"),
    "buzzfeednews": re.compile("\/article\/"),
    "fayettevilleflyer": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "henricocitizen": re.compile("\/articles\/"),
    "jezebel": re.compile("^(?:(?!\/amp|\.js|img\.).)+$"),
    "journalgazette": re.compile("(2010|2015|2020)11[0-9]{2}"),
    "latimes": re.compile("(2010|2015|2020)-11-[0-9]{2}"),
    "mic": re.compile("^(?:(?!\.jpg|\.js).)+$"),
    "nypost": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "nytimes": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "politico": re.compile("(2020|2015|2010)\/11"),
    "usatoday": re.compile("(2020|2015|2010)\/11\/[0-9]{2}"),
    "vox": re.compile("(2020|2015|2010)\/11\/[0-9]{1,2}"),
    "wsj": re.compile("\/articles\/")
}

SELECTORS = {
    "auburnexaminer": {
        "hed": "meta[property='og:title']",
        "authors": "div.post-byline",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.post-content",
    },
    "blockclubchicago": {
        "hed": "meta[property='og:title']",
        "authors": "a.post-byline__author-link",
        "pub_date": "meta[property='article:published_time']",
        "body": "div[class*='articleContent__Wrapper']",
    },
    "buzzfeednews": {
        "hed": "meta[property='og:title']",
        "authors": "span[class*='bylineName']",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.subbuzzes-wrapper",
    },
    "fayettevilleflyer": {
        "hed": "meta[property='og:title']",
        "authors": "a[title*='Posts by']",
        "pub_date": "ul.entry-meta > li",
        "body": "div.text",
    },
    "henricocitizen": {
        "hed": "meta[property='og:title']",
        "authors": "a.byline__author",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.entry-content",
    },
    "jezebel": {
        "hed": "meta[property='og:title']",
        "authors": "a[data-ga*='Author']",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.js_post-content",
    },
    "journalgazette": {
        "hed": "meta[property='og:title']",
        "authors": "span[itemprop='author']",
        "pub_date": "time",
        "body": "div#article-body",
    },
    "latimes": {
        "hed": "meta[property='og:title']",
        "authors": "div.author-name",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.rich-text-article-body",
    },
    "mic": {
        "hed": "meta[property='og:title']",
        "authors": "a.NU",
        "pub_date": "time",
        "body": "div.Af",
    },
    "nypost": {
        "hed": "meta[property='og:title']",
        "authors": "div.byline__author",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.entry-content",
    },
    "nytimes": {
        "hed": "meta[property='og:title']",
        "authors": "span[itemprop='name']",
        "pub_date": "meta[property='article:published_time']",
        "body": "section[name='articleBody']",
    },
    "politico": {
        "hed": "meta[property='og:title']",
        "authors": "p.story-meta__authors > span",
        "pub_date": "time",
        "body": "div.story-text",
    },
    "usatoday": {
        "hed": "meta[property='og:title']",
        "authors": "a.gnt_ar_by_a",
        "pub_date": "div.gnt_ar_dt",
        "body": "div.gnt_ar_b",
    },
    "vox": {
        "hed": "meta[property='og:title']",
        "authors": "span.c-byline__author-name",
        "pub_date": "meta[property='article:published_time']",
        "body": "div.c-entry-content",
    },
    "wsj": {
        "hed": "meta[property='og:title']",
        "authors": "a[class*='Author__AuthorLink']",
        "pub_date": "time",
        "body": "section[class*='ArticleBody__Container']",
    },
}

class FulltextEngine:
    def __init__(
        self,
        db: Database,
        sitename: str,
    ) -> None:
        self.db = db
        self.name = sitename
        self.regex_pattern = PATTERNS[self.name]
        self.element_selectors = SELECTORS[self.name]
    
    @property
    def filtered_url_set(self):
        url_set = self.db.get_url_superset()
        url_set_filtered = self.db.clean_urls(url_set, self.regex_pattern)
        url_set_parsed = self.db.get_urls_from_table("parsed_articles")
        url_set_errored = self.db.get_urls_from_table("errors")
        url_set_to_collect = url_set_filtered - url_set_parsed - url_set_errored
        return url_set_to_collect

    def _save_results(self, result: Dict) -> None:
        rdf = pd.DataFrame([result])
        self.db.save_table(rdf, "parsed_articles", append=True)

    def scrape_filtered_urls(self) -> None:
        """
        Drives Playwright scraping loop for article URLs in filtered set
        """
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            for u in tqdm(self.filtered_url_set):
                page.goto(u)

                hed = page.query_selector(self.element_selectors["hed"])
                if "meta" in self.element_selectors["hed"]:
                    hed = hed.get_attribute("content")
                else:
                    hed = hed.inner_text()
                
                pub_date = page.query_selector(self.element_selectors["pub_date"])
                if "meta" in self.element_selectors["pub_date"]:
                    pub_date = pub_date.get_attribute("content")
                elif self.sitename=="usatoday":
                    pub_date = pub_date.get_attribute("aria-label")
                else:
                    pub_date = pub_date.inner_text()
                
                authors = [i.inner_text() for i in page.query_selector_all(self.element_selectors["authors"])]
                body = [i.inner_text() for i in page.query_selector_all(self.element_selectors["body"])]
                result = {
                    "hed": hed,
                    "pub_date": pub_date,
                    "authors": authors,
                    "body": body,
                }
                self._save_results(result)
                
            browser.close()