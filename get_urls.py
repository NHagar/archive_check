import argparse
import logging
import os
import pathlib
import sqlite3

from pipeline.collect import Site
from pipeline.db import Database

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level = logging.INFO)

parser = argparse.ArgumentParser(description='Run URL collection for a set of domains.')
parser.add_argument("--sites", type=str, required=True, help="comma-separated list of domains to collect (e.g., nytimes.com,wsj.com")
parser.add_argument("--start", type=str, required=True, help="start date, of format Y-M-D")
parser.add_argument("--end", type=str, required=True, help="end date, of format Y-M-D")
args = parser.parse_args()

SITES = [i.strip() for i in args.sites.split(",")]
START = args.start
END = args.end
DATA_PATH = pathlib.Path("./data")
MC_API_KEY = os.environ.get("API_KEY_MC")

# For each domain
logging.info(f"Collecting records for {len(SITES)} sites, from {START} to {END}.")
for s in SITES:
    sname = s.split(".")[0]
    # Database check/creation
    logging.info(f"Connecting to database for {sname}")
    con = sqlite3.connect(DATA_PATH / f"{sname}.db")
    database = Database(con)
    site = Site(s, START, END)

    if not database.checkTableExists("wayback"):
        logging.info(f"Collecting wayback machine records for {sname}")
        wayback = site.archive_query()
        database.save_table(wayback, "wayback", append=False)    

    if not database.checkTableExists("gdelt"):
        logging.info(f"Collecting GDELT records for {sname}")
        gdelt = site.gdelt_query()
        database.save_table(gdelt, "gdelt", append=False)

    if not database.checkTableExists("mediacloud"):
        logging.info(f"Collecting Media Cloud records for {sname}")
        mc = site.mediacloud_query(MC_API_KEY)
        database.save_table(mc, "mediacloud", append=False)