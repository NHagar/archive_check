import argparse
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
for s in SITES:
    sname = s.split(".")[0]
    # Database check/creation
    con = sqlite3.connect(DATA_PATH / f"{sname}.db")
    # For each service
        # URL collection
        # Table check/creation 
    site = Site(s, START, END)
    wayback = site.archive_query()
    wayback.to_sql("wayback", con)
    gdelt = site.gdelt_query()
    gdelt.to_sql("gdelt", con)
    mc = site.mediacloud_query(MC_API_KEY)
    mc.to_sql("mediacloud", con)