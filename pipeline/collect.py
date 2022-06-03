from datetime import datetime
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests
from tqdm import tqdm

class Site:
    """Parameters and queries for a site's archives across services
    """
    def __init__(self, domain: str, start: str, end: str) -> None:
        self.domain = domain
        self.start_date = datetime.strptime(start, "%Y-%m-%d")
        self.end_date = datetime.strptime(end, "%Y-%m-%d")
        self.start = start
        self.end = end
        self.mc_ids = pd.read_csv("./data/mediacloud_sources.csv")

    def __mediacloud_lookup(self) -> pd.DataFrame:
        """helper function to find mediacloud domain IDs
        """
        domain_variants = ["http://", "https://", "http://www.", "https://www."]
        domain_variants = [i+self.domain for i in domain_variants]
        domain_variants.extend([i+"/" for i in domain_variants])
        media_id = ""
        for k,v in self.mc_ids.iterrows():
            if v['url'].split("#")[0] in domain_variants:
                media_id = f"media_id:{str(v['media_id'])}"
                break

        return media_id

    def __gdelt_file_index(self) -> "list[str]":
        """Helper function to get list of GDELT mentions files
        """
        index_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
        r = requests.get(index_url)
        lines = r.text.split("\n")
        exports = [i.split(" ")[-1] for i in lines]
        exports = [i for i in exports if "mentions" in i]
        return exports

    def __gdelt_filter(self) -> "list[str]":
        """Helper function to filter GDELT URLs
        """
        mentions_urls = self.__gdelt_file_index()

        date_range = pd.date_range(start=self.start_date, end=self.end_date)
        date_range_encoded = [10000*dt_time.year + 100*dt_time.month + dt_time.day for dt_time in date_range]
        date_range_encoded = [f"/{i}" for i in date_range_encoded]
        relevant_urls = []
        for i in date_range_encoded:
            date_urls = [u for u in mentions_urls if i in u]
            relevant_urls.extend(date_urls)
        
        return relevant_urls
    
    def archive_query(self) -> pd.DataFrame:
        """get internet archive records for associated timeframe/domain
        """
        start_date = 10000*self.start_date.year + 100*self.start_date.month + self.start_date.day
        # IA request needs to be more expansive bc of collection window
        today = datetime.today()
        end_date = 10000*today.year + 100*today.month + today.day
        url = 'https://web.archive.org/cdx/search/cdx'
        params = {"url": self.domain,
                "matchType": "domain",
                "filter": "mimetype:text/html",
                "from": start_date,
                "to": end_date,
                "output": "json"}
        headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"}
        response = requests.get(url, params=params, headers=headers).json()
        results = [{"timestamp": i[1], "url": i[2]} for i in response[1:]]
        results = pd.DataFrame(results)

        return results
    
    def gdelt_query(self) -> pd.DataFrame:
        """get gdelt records for associated timeframe/domain
        """
        # TODO: Can we cache this process somehow, so we're not repeatedly processing the same files for all sites?
        urls = self.__gdelt_filter()        
        df_all = []
        for i in tqdm(urls):
            page = requests.get(i)
            zipfile = ZipFile(BytesIO(page.content))
            filename = zipfile.namelist()[0]
            df = pd.read_csv(zipfile.open(filename), sep='\t', header=None, low_memory=False)
            df_filtered = df.loc[df[4]==self.domain, [2,5]]
            df_all.append(df_filtered)
        try:
            df_all = pd.concat(df_all).drop_duplicates()
            df_all = df_all.rename(columns={2: "timestamp", 5: "url"})
        except ValueError:
            df_all = None
        return df_all
    
    def mediacloud_query(self, api_key: str) -> pd.DataFrame:
        """get mediacloud records for associated timeframe/domain
        """
        media_id = self.__mediacloud_lookup()
        start_date_mc = str(self.start_date).replace(" ", "T") + "Z" 
        end_date_mc = str(self.end_date).replace(" ", "T") + "Z"

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
            r = requests.get( 'https://api.mediacloud.org/api/v2/stories_public/list/', params = params, headers = { 'Accept': 'application/json'} )
            stories = r.json()

            if len(stories) == 0:
                break

            start = stories[-1]['processed_stories_id']

            all_stories.extend(stories)
        stories_df = pd.DataFrame(all_stories)
        try:
            stories_df = stories_df.drop(columns=['story_tags'])
        except KeyError:
            pass
        return stories_df