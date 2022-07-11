from dataclasses import dataclass
import logging
import re
import sqlite3

import pandas as pd


@dataclass
class Database:
    """Class for database info and operations
    """
    con: sqlite3.Connection

    def list_tables(self) -> "list[str]":
        """Generate list of table names in database
        """
        cursor = self.con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [i[0] for i in cursor.fetchall()]
        cursor.close()

        return tables

    def get_url_superset(self) -> "set[str]":
        """Get full list of URLs across tables, no cleaning
        """
        tables = [i for i in self.list_tables() if i not in  ["parsed_articles", "errors"]]
        tab_list = []
        for t in tables:
            try:
                t = pd.read_sql_query(f"SELECT url FROM {t}", self.con)
                tab_list.append(t)
            except pd.io.sql.DatabaseError:
                pass
        urls = set(pd.concat(tab_list).url)

        return urls

    def get_urls_from_table(self, tablename: str) -> "set[str]":
        """Get unique URLs stored in a table
        """
        cursor = self.con.cursor()
        cursor.execute(f'''SELECT count(name) 
                           FROM sqlite_master 
                           WHERE type='table' AND name='{tablename}' ''')
        if cursor.fetchone()[0]:
            try:
                urls = pd.read_sql_query(
                    f"SELECT url FROM {tablename}", self.con).url.tolist()
                urls = set(urls)
            except pd.io.sql.DatabaseError:
                logging.warn(f"Table {tablename} is empty.")
                urls = set()
        else:
            logging.warn(f"Table {tablename} does not exist.")
            urls = set()

        return urls

    def clean_urls(self, urls: "set[str]", pattern: re.Pattern) -> "set[str]":
        """Clean list of URLs according to regex pattern
        """
        onsite = self.get_urls_from_table("onsite")
        patterned = [i for i in urls if pattern.search(
            i) is not None or i in onsite]
        cleaned = set([i.split("?")[0] for i in patterned])

        return cleaned

    # https://stackoverflow.com/questions/17044259/python-how-to-check-if-table-exists/17044893
    def checkTableExists(self, tablename: str) -> bool:
        dbcur = self.con.cursor()
        dbcur.execute("""
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE name = '{0}'
            """.format(tablename.replace('\'', '\'\'')))
        if dbcur.fetchone()[0] == 1:
            dbcur.close()
            logging.warn(
                f"Table {tablename} already exists. Skipping this step.")
            return True

        dbcur.close()
        return False

    def save_table(self, df: pd.DataFrame, tablename: str, append: bool = True) -> None:
        """Overwrite or append dataframe to table
        """
        logging.info(f"Found {len(df)} records. Saving to table")
        table_has_records = len(self.read_table(tablename)) > 0
        if append and table_has_records:
            behavior = "append"
        else:
            behavior = "replace"
        df.to_sql(tablename, self.con, if_exists=behavior)

    def read_table(self, tablename: str) -> pd.DataFrame:
        """Read table to dataframe
        """
        try:
            df = pd.read_sql_query(f"SELECT * FROM {tablename}", self.con)
            return df
        except pd.io.sql.DatabaseError:
            return pd.DataFrame()

    def join_to_parsed_clean(self, tablename: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Join selected table to parsed articles table,
        filter to specified time period
        """
        try:
            df = pd.read_sql_query(
                f"SELECT {tablename}.url, url_canonical, text, hed, pub_date \
                    FROM {tablename} \
                        LEFT JOIN parsed_articles \
                            ON parsed_articles.url = {tablename}.url", self.con)
            start = pd.to_datetime(start_date, utc=True)
            end = pd.to_datetime(end_date, utc=True)
            # Convert and filter datetime
            df.loc[:, "pub_date"] = pd.to_datetime(df.pub_date, utc=True)
            df = df[(df.pub_date>=start) & (df.pub_date<=end)]
            df = df.drop_duplicates()

            return df
        except pd.io.sql.DatabaseError:
            logging.warn(f"Table {tablename} is empty.")
            return pd.DataFrame()

    def table_to_csv(self, tablename: str, filepath: str) -> None:
        """saves database table to CSV file
        """
        df = self.read_table(tablename)
        df.to_csv(filepath)
        print(f"Table {tablename} saved to {filepath}")