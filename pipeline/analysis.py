from dataclasses import dataclass

import pandas as pd

@dataclass
class Table:
    name: str
    df: pd.DataFrame

    def clean_dataframe(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Common analysis preprocessing steps
        """
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        # Get rid of failed joins
        df = self.df[(~self.df.text.isna()) & (~self.df.pub_date.isna())]
        # Convert and filter datetime
        df.loc[:, "pub_date"] = pd.to_datetime(df.pub_date, utc=True)
        df = df[(df.pub_date>=start) & (df.pub_date<=end)]

        return df

    def count_urls(df: pd.DataFrame) -> int:
        """Count unique URLs in dataframe
        """
        urls = len(df.url_canonical.unique())

        return urls