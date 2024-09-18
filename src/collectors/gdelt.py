import zipfile
from pathlib import Path

import pandas as pd
import requests


class GdeltCollector:
    def __init__(self):
        self.cache_dir = Path(".cache/gdelt")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_file_index(self, start, end):
        start_str_encoded = str(10000 * start.year + 100 * start.month + start.day)
        end_str_encoded = str(10000 * end.year + 100 * end.month + end.day)
        index_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
        r = requests.get(index_url)
        lines = r.text.split("\n")
        export_urls = [i.split(" ")[-1] for i in lines if "mentions" in i]

        export_urls_filtered = []

        for i in export_urls:
            dt = i.split("/")[-1].split(".")[0]
            if dt >= start_str_encoded and dt <= end_str_encoded:
                export_urls_filtered.append(i)

        return export_urls_filtered

    def download_file(self, url, cache=True):
        filename = url.split("/")[-1]
        path = self.cache_dir / filename
        if not path.exists():
            r = requests.get(url)
            path.write_bytes(r.content)
        return path

    def file_to_dataframe(self, fpath):
        with zipfile.ZipFile(fpath, "r") as z:
            filename = z.namelist()[0]
            with z.open(filename) as f:
                df = pd.read_csv(f, sep="\t", header=None)

        # set column names from GDELT codebook http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
        df.columns = [
            "GlobalEventID",
            "EventTimeDate",
            "MentionTimeDate",
            "MentionType",
            "MentionSourceName",
            "MentionIdentifier",
            "SentenceID",
            "Actor1CharOffset",
            "Actor2CharOffset",
            "ActionCharOffset",
            "InRawText",
            "Confidence",
            "MentionDocLen",
            "MentionDocTone",
            "MentionDocTranslationInfo",
            "Extras",
        ]

        return df

    def download_files(self, start, end):
        urls = self.get_file_index(start, end)
        fpaths = []
        for url in urls:
            file_fpath = self.download_file(url)
            fpaths.append(file_fpath)

        return fpaths

    def load_files(self):
        files = list(self.cache_dir.glob("*.zip"))
        dfs = []
        for f in files:
            df = self.file_to_dataframe(f)
            dfs.append(df)
        return pd.concat(dfs)
