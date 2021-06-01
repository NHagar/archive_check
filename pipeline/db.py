import logging
import sqlite3

import pandas as pd

# https://stackoverflow.com/questions/17044259/python-how-to-check-if-table-exists/17044893
def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
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

def save_table(df: pd.DataFrame, tablename: str, con: sqlite3.Connection) -> None:
    logging.info(f"Found {len(df)} records. Saving to table")
    df.to_sql(tablename, con)