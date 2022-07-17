import pathlib
import sqlite3

from pipe.db import Database
from pipe.selectors import FulltextEngine

data_path = pathlib.Path("./data")

dbs = sorted(list(data_path.glob("*.db")))

for d in dbs:
    # Set up database connection
    con = sqlite3.connect(d)
    db = Database(con)
    # Load and clean URLs
    sitename = d.name.replace(".db", "")
    ft = FulltextEngine(db, sitename)
    ft.scrape_filtered_urls()