import argparse
import logging
import os
import pathlib
import sqlite3

from pipeline.collect import Site

from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description='Run URL collection for a set of domains.')
parser.add_argument("--sites", type=str, required=True, help="comma-separated list of domains to collect")
parser.add_argument("--start", type=str, required=True, help="start date, of format Y-M-D")
parser.add_argument("--end", type=str, required=True, help="end date, of format Y-M-D")
args = parser.parse_args()

SITES = [i.strip() for i in args.sites.split(",")]
START = args.start
END = args.end
DATA_PATH = pathlib.Path("./data")
MC_API_KEY = os.environ.get("API_KEY_MC")

# For each domain
logging.info(f"Collecting records for f{len(SITES)} sites, from {START} to {END}.")
for s in SITES:
    sname = s.split(".")[0]
    # Database check/creation
    logging.info(f"Making database for {sname}")
    con = sqlite3.connect(DATA_PATH / f"{sname}.db")
    # For each service
        # URL collection
        # Table check/creation 
    site = Site(s, START, END)
    logging.info(f"Collecting wayback machine records for {sname}")
    wayback = site.archive_query()
    logging.info(f"Found {len(wayback)} records. Saving to table")
    wayback.to_sql("wayback", con)
    logging.info(f"Collecting GDELT records for {sname}")
    gdelt = site.gdelt_query()
    logging.info(f"Found {len(gdelt)} records. Saving to table")
    gdelt.to_sql("gdelt", con)
    logging.info(f"Collecting Media Cloud records for {sname}")
    mc = site.mediacloud_query(MC_API_KEY)
    logging.info(f"Found {len(mc)} records. Saving to table")
    mc.to_sql("mediacloud", con)