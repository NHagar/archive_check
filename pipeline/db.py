def save_table(df: pd.DataFrame, tablename: str, con: sqlite3.Connection) -> None:
    logging.info(f"Found {len(df)} records. Saving to table")
    try:
        df.to_sql(tablename, con, if_exists="fail")
    except ValueError:
        logging.warn(f"Table {tablename} already exists. Skipping this step.")