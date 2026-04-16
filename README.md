# wikipedia-page-views

English Wikipedia page view statistics sourced from Wikimedia's [pageview complete dumps](https://dumps.wikimedia.org/other/pageview_complete/).

## Coverage

- **Source**: `pageviews-YYYYMMDD-user.bz2` daily dumps (user-agent traffic only, bots excluded)
- **Scope**: English Wikipedia (`en.wikipedia`) pages with a valid `page_id`
- **History**: 2016-01-01 to present (ingested incrementally, one day per file)
- **Not included**: Non-English Wikipedias, bot traffic, pages without a stable page_id

## Datasets

| Dataset | Rows | Description |
|---------|------|-------------|
| `wikipedia_top_pages_monthly` | ~12M | Monthly views for the top 100k pages by all-time total views |
| `wikipedia_top_pages_daily_recent` | ~900k | Daily views for the top 10k pages over the last 90 days |

## Architecture

The connector uses a two-node DAG:

1. **page_views** (ingest) — Downloads daily bz2 dumps, decompresses, filters to `en.wikipedia`, saves one parquet per day. Runs with a 5.8-hour time budget and re-triggers if more dates remain.
2. **transform** — Builds both publishable datasets using DuckDB. Only runs once ingest has caught up to yesterday. Skips if no new data since last transform.

## Notes

- Ingest is resumable: state tracks completed dates, so interrupted runs pick up where they left off.
- Each daily dump is ~500MB compressed. Full historical ingest (~3,700 days) requires multiple continuation runs.
- Transform uses `overwrite` (not `merge`) because the ranking changes with each new day of data.
