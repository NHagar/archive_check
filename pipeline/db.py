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

    def list_tables(self) -> list[str]:
        """Generate list of table names in database
        """
        cursor = self.con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [i[0] for i in cursor.fetchall()]
        cursor.close()

        return tables
    
    def get_url_superset(self) -> set[str]:
        """Get full list of URLs across tables, no cleaning/dedupe
        """
        tables = [i for i in self.list_tables() if i!="parsed_articles"]
        tab_list = []
        for t in tables:
            t = pd.read_sql_query(f"SELECT url FROM {t}", self.con)
            tab_list.append(t)
        
        urls = set(pd.concat(tab_list).url)

        return urls

    def get_urls_from_table(self, tablename: str) -> set[str]:
        """Get unique URLs stored in a table
        """
        cursor = self.con.cursor()
        cursor.execute(f'''SELECT count(name) 
                           FROM sqlite_master 
                           WHERE type='table' AND name='{tablename}' ''')
        if cursor.fetchone()[0]:
            urls = pd.read_sql_query(f"SELECT url FROM {tablename}", self.con).url.tolist()
            urls = set(urls)
        else:
            logging.warn(f"Table {tablename} does not exist.")
            urls = set()

        return urls
    
    def clean_urls(self, urls:set[str], pattern: re.Pattern) -> set[str]:
        """Clean list of URLs according to regex pattern
        """
        patterned = [i for i in urls if pattern.search(i) is not None]
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
            logging.warn(f"Table {tablename} already exists. Skipping this step.")
            return True

        dbcur.close()
        return False

    def save_table(self, df: pd.DataFrame, tablename: str, append: bool=True) -> None:
        """Overwrite or append dataframe to table
        """
        logging.info(f"Found {len(df)} records. Saving to table")
        if append:
            behavior = "append"
        else:
            behavior = "replace"
        df.to_sql(tablename, self.con, if_exists=behavior)