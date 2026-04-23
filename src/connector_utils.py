"""Wikipedia Page Views connector utilities.

Shared helpers for the Wikipedia page views ingest and transform nodes.
"""

import os
from datetime import datetime, timedelta

import duckdb as ddb
from subsets_utils.config import is_cloud, raw_uri


def connect_duckdb() -> ddb.DuckDBPyConnection:
    """Open a DuckDB connection configured for R2 in cloud mode."""
    con = ddb.connect()
    if is_cloud():
        con.sql(f"""
            SET s3_endpoint='{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com';
            SET s3_access_key_id='{os.environ['R2_ACCESS_KEY_ID']}';
            SET s3_secret_access_key='{os.environ['R2_SECRET_ACCESS_KEY']}';
            SET s3_region='auto';
        """)
    return con


def all_parquets_glob() -> str:
    """Glob URI covering every ingested daily parquet."""
    return raw_uri("page_views_*", "parquet")


def recent_parquet_uris(last_ingested: str, window_days: int) -> list[str]:
    """Explicit list of parquet URIs for the last `window_days` days
    ending at the given ingested date (inclusive). Every listed file must
    exist -- ingest is known to have processed them.
    """
    last = datetime.strptime(last_ingested, "%Y-%m-%d").date()
    start = last - timedelta(days=window_days - 1)
    uris = []
    d = start
    while d <= last:
        uris.append(raw_uri(f"page_views_{d.strftime('%Y%m%d')}", "parquet"))
        d += timedelta(days=1)
    return uris


def asset_id_for_date(date: datetime) -> str:
    """Return the raw asset ID for a given date."""
    return f"page_views_{date.strftime('%Y%m%d')}"
