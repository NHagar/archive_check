import pandas as pd


def clean_dataframe(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Common analysis preprocessing steps
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    # Get rid of failed joins
    df = df[(~df.text.isna()) & (~df.pub_date.isna())]
    # Convert and filter datetime
    df.loc[:, "pub_date"] = pd.to_datetime(df.pub_date, utc=True)
    df = df[(df.pub_date>=start) & (df.pub_date<=end)]

    return df