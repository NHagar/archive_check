from dataclasses import dataclass
import re
import sqlite3

import pandas as pd

@dataclass
class Database:
    """Class for database info and operations
    """
    con: sqlite3.Connection
    pattern: re.Pattern

    def list_tables(self) -> list[str]:
        cursor = self.con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [i[0] for i in cursor.fetchall()]
        cursor.close()

        return tables
    
    def get_url_superset(self) -> set[str]:
        tables = self.list_tables()
        tab_list = []
        for t in tables:
            t = pd.read_sql_query(f"SELECT url FROM {t}", self.con)
            tab_list.append(t)
        
        urls = set(pd.concat(tab_list).url)

        return urls
    
    def clean_url_superset(self, urls:set[str]) -> set[str]:
        p = self.pattern
        patterned = [i for i in urls if p.search(i) is not None]
        cleaned = set([i.split("?")[0] for i in patterned])
        
        return cleaned