from dataclasses import dataclass

import pandas as pd

@dataclass
class Table:
    name: str
    df: pd.DataFrame

    def count_urls(self) -> int:
        """Count unique URLs in dataframe
        """
        urls = len(self.df.url_canonical.unique())

        return urls