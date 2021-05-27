import math
from datetime import datetime, timedelta
import requests
import pandas as pd
import mediacloud.api
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from newsapi import NewsApiClient, newsapi_exception
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from tqdm import tqdm
import aiohttp
import asyncio

def archive_query(domain, start_date, end_date):
    start_date = 10000*start_date.year + 100*start_date.month + start_date.day
    end_date = 10000*end_date.year + 100*end_date.month + end_date.day
    url = 'https://web.archive.org/cdx/search/cdx'
    params = {"url": domain,
            "matchType": "domain",
            "from": start_date,
            "to": end_date,
            "output": "json"}
    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"}
    response = requests.get(url, params=params, headers=headers).json()
    results = [{"timestamp": i[1], "url": i[2]} for i in response[1:]]
    results = pd.DataFrame(results)

    return results


def gdelt_query(domain, start_date, end_date):
    url = 'http://data.gdeltproject.org/events/'
    date_range = pd.date_range(start=start_date, end=end_date)
    date_range_encoded = [10000*dt_time.year + 100*dt_time.month + dt_time.day for dt_time in date_range]
    uris = ['{}.export.CSV.zip'.format(i) for i in date_range_encoded]
    urls = ['{0}{1}'.format(url, i) for i in uris]

    df_all = []

    for i in urls:
        page = urlopen(i)
        zipfile = ZipFile(BytesIO(page.read()))
        filename = zipfile.namelist()[0]
        df = pd.read_csv(zipfile.open(filename), sep='\t', header=None, low_memory=False)
        site_links = df[df[57].str.contains(domain)][57]
        df_filtered = pd.DataFrame({'url': site_links, 'timestamp': filename})
        df_filtered.loc[:, 'timestamp'] = df_filtered['timestamp'].map(lambda x: x.split(".")[0])
        df_all.append(df_filtered)
    
    df_all = pd.concat(df_all).drop_duplicates()
    return df_all

# TODO: Implement day-to-day pagination to get most possible results
def newsapi_query(domain, start_date, end_date, api_key):
    newsapi = NewsApiClient(api_key=api_key)
    try:
        articles = newsapi.get_everything(domains=domain,
                                        from_param=start_date,
                                        to=end_date,
                                        page_size=100)
    except newsapi_exception.NewsAPIException as e:
        if e.get_code()=="parameterInvalid":
            start_date = datetime.now().date() - timedelta(days=30)
            print(f"""NEWSAPI: The free plan doesn't go back that far. Your new query ranges from
                {start_date} to {end_date}""")
            articles = newsapi.get_everything(domains=domain,
                                            from_param=start_date,
                                            to=end_date,
                                            page_size=100)
        else:
            print(f"NEWSAPI: {e}")

    starting_articles = articles['articles']
    additional_records = articles['totalResults'] - 100
    pages = range(2, math.ceil(additional_records/100) + 1)
    print(f"NEWSAPI: There are {additional_records if additional_records > 0 else 0} additional records")
    try:
        for i in pages:
            print(f"NEWSAPI: page {i}")
            extra_articles = newsapi.get_everything(domains=domain,
                                                    from_param=start_date,
                                                    to=end_date,
                                                    page_size=100,
                                                    page=i)
            starting_articles.extend(extra_articles['articles'])
    except newsapi_exception.NewsAPIException as e:
        if e.get_code()=="maximumResultsReached":
            print(f"NEWSAPI: {e.get_message()}")
        else:
            print(f"NEWSAPI: {e}")


    all_articles = pd.DataFrame(starting_articles)
    return all_articles



def mediacloud_query(domain, start_date, end_date, api_key):
    media_id = mediacloud_lookup(domain)
    start_date_mc = str(start_date).replace(" ", "T") + "Z" 
    end_date_mc = str(end_date).replace(" ", "T") + "Z"

    all_stories = []
    start = 0
    rows  = 100
    while True:
        params = {
            'last_processed_stories_id': start,
            'rows': rows,
            'q': f'{media_id}',
            'fq': f'publish_date:[{start_date_mc} TO {end_date_mc}]',
            'key': api_key
        }

        print("Fetching {} stories starting from {}".format(rows, start))
        r = requests.get( 'https://api.mediacloud.org/api/v2/stories_public/list/', params = params, headers = { 'Accept': 'application/json'} )
        stories = r.json()

        if len(stories) == 0:
            break

        start = stories[-1]['processed_stories_id']

        all_stories.extend(stories)
    stories_df = pd.DataFrame(all_stories)
    return stories_df


def mediacloud_lookup(domain):
    mc_sources = pd.read_csv("./data/mediacloud_sources.csv")
    domain_variants = ["http://", "https://", "http://www.", "https://www."]
    domain_variants = [i+domain for i in domain_variants]
    domain_variants.extend([i+"/" for i in domain_variants])
    media_id = ""
    for k,v in mc_sources.iterrows():
        if v['url'] in domain_variants:
            media_id = f"media_id:{str(v['media_id'])}"
            break

    return media_id


async def get_records(url, domain):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            records = []
            resp = await response.content.readchunk()
            for record in ArchiveIterator(resp, arc2warc=True):
                if record.rec_type == 'response':
                    if record.http_headers.get_header('Content-Type') == 'text/html':
                        uri = record.rec_headers.get_header('WARC-Target-URI')
                        if domain in uri:
                            records.append(uri)
            return records


def cc_query(domain, start_date, end_date):
    year = str(start_date.year)
    month = str(start_date.month)
    month = month if len(month)>1 else "0{}".format(month)
    cc_files = requests.get("https://commoncrawl.s3.amazonaws.com/?prefix=crawl-data/CC-NEWS/{0}/{1}".format(year, month))
    soup = BeautifulSoup(cc_files.text)
    urls = ["https://commoncrawl.s3.amazonaws.com/{}".format(i.text) for i in soup.find_all("key")]
    loop = asyncio.get_event_loop()
    coroutines = [get_records(i, domain) for i in urls]
    results = loop.run_until_complete(asyncio.gather(*coroutines))

    return results