import argparse

from pipeline.collect import Site

parser = argparse.ArgumentParser(description='Run URL collection for a set of domains.')
parser.add_argument("--sites", type=str, required=True)
SITES = parser.parse_args().sites.split(",")



# For each domain
    # Database check/creation
    # For each service
        # URL collection
        # Table check/creation 