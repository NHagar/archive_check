This project provides a CLI for collecting news data. Given a timeframe and a set of top-level domains, the tool looks for corresponding news articles in the Wayback Machine, GDELT, and Media Cloud.

# Installation

`git clone https://github.com/NHagar/archive_check.git`

`pip install -r requirements.txt`

# Usage
`python get_urls.py` retrieves URLs for designated domains and date range, then saves to databases. Queries Wayback Machine, GDELT, and Media Cloud

**parameters**
`--sites` comma-separated list of domains to query, of format: nytimes.com,latimes.com,vox.com
`--start` desired start date, of format "YYYY-MM-DD"
`--end` desired end date, of format "YYYY-MM-DD"

`scripts/` contains example one-off scripts for collecting data directly from sites.

`python get_fulltext.py` attempts to scrape full text from all URLs in database. `PATTERNS` can be modified to apply site-specific URL filtering heuristics before scraping.

`python compare_results.py` runs set of analyses (link count, LDA, headline regression) for each database table and outputs result comparisons across services. Links that 404 in the fulltext collection step won't be counted here.

# Requirements

Media Cloud API key (in root `.env` file, called `API_KEY_MC`)
