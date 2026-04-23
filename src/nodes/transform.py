"""Build publishable Wikipedia page view datasets.

Two outputs:

1. wikipedia_top_pages_monthly — monthly views for the top 100k pages ranked by
   their total views across the full ingested window (~10 years). ~12M rows,
   sorted by (page_id, month) so per-entity point lookups read only a tiny
   fraction of the file via parquet row-group pruning.

2. wikipedia_top_pages_daily_recent — daily views for the top 10k pages of the
   last 90 ingested days. Companion to the monthly table for "what's hot now"
   charts. ~900k rows, sorted by (page_id, date).

Scheduling: transform only runs when ingest has caught up to yesterday and
has new data since the last successful transform. Otherwise it is a no-op, so
it is safe to wire as a dependency of the daily ingest workflow — during
ingest continuation runs it just skips.
"""
from datetime import datetime, timedelta

from connector_utils import connect_duckdb, all_parquets_glob, recent_parquet_uris
from subsets_utils import (
    load_state, save_state, overwrite, publish, validate,
)

from nodes.page_views import run as ingest_run


TOP_MONTHLY = 100_000
TOP_DAILY = 10_000
DAILY_WINDOW_DAYS = 90


MONTHLY_METADATA = {
    "id": "wikipedia_top_pages_monthly",
    "title": "Wikipedia Top 100k Pages — Monthly Views",
    "description": (
        "Monthly English Wikipedia page view counts for the 100,000 pages with "
        "the highest total views across the full ingested window (~10 years). "
        "Page identity is tracked by the stable Wikipedia page_id; the entity "
        "column reflects the most recent page title seen for that id. Sorted by "
        "(page_id, month) for efficient per-entity queries."
    ),
    "license": "CC BY-SA 4.0",
    "column_descriptions": {
        "month": "Month of observation, formatted as YYYY-MM.",
        "page_id": "Stable Wikipedia page identifier.",
        "entity": "Page title (URL-encoded form) as of the most recent observation.",
        "views": "Sum of English Wikipedia page views in the month (user agents only).",
    },
}

DAILY_METADATA = {
    "id": "wikipedia_top_pages_daily_recent",
    "title": "Wikipedia Top 10k Pages — Recent Daily Views",
    "description": (
        f"Daily English Wikipedia page view counts for the 10,000 pages with the "
        f"highest total views over the last {DAILY_WINDOW_DAYS} ingested days. "
        f"Rebuilt from scratch on each transform run. Sorted by (page_id, date)."
    ),
    "license": "CC BY-SA 4.0",
    "column_descriptions": {
        "date": "Date of observation, formatted as YYYY-MM-DD.",
        "page_id": "Stable Wikipedia page identifier.",
        "entity": "Page title (URL-encoded form) as of the most recent observation in the window.",
        "views": "English Wikipedia page views for that day (user agents only).",
    },
}


def _build_monthly(con):
    """Two-pass monthly build.

    Pass 1: aggregate total views + latest entity per page_id across the full
            window, keep the top 100k.
    Pass 2: re-scan the full window, filter to those page_ids, aggregate per
            (month, page_id). Sorted by (page_id, month) for pruning.
    """
    glob = all_parquets_glob()

    print(f"[monthly] Pass 1: ranking top {TOP_MONTHLY:,} pages by total views")
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE top_pages AS
        SELECT
            page_id,
            ARG_MAX(entity, date) AS entity,
            SUM(views) AS total_views
        FROM read_parquet('{glob}')
        GROUP BY page_id
        ORDER BY total_views DESC
        LIMIT {TOP_MONTHLY}
    """)
    ranked = con.sql("SELECT COUNT(*) FROM top_pages").fetchone()[0]
    print(f"[monthly] Pass 1 done: {ranked:,} pages")

    print("[monthly] Pass 2: aggregating monthly views for top pages")
    table = con.sql(f"""
        SELECT
            SUBSTR(p.date, 1, 7) AS month,
            t.page_id,
            t.entity,
            SUM(p.views) AS views
        FROM read_parquet('{glob}') p
        INNER JOIN top_pages t ON p.page_id = t.page_id
        GROUP BY month, t.page_id, t.entity
        ORDER BY t.page_id, month
    """).arrow()
    print(f"[monthly] Pass 2 done: {len(table):,} rows")
    return table


def _build_daily_recent(con, last_ingested: str):
    """Daily view for the top 10k pages in the last 90 ingested days.

    Reads only the recent parquets (not the full window) so it is cheap.
    Sorted by (page_id, date).
    """
    uris = recent_parquet_uris(last_ingested, DAILY_WINDOW_DAYS)
    uri_list = "[" + ", ".join(f"'{u}'" for u in uris) + "]"

    print(f"[daily] ranking top {TOP_DAILY:,} pages over last {DAILY_WINDOW_DAYS} days ({len(uris)} files)")
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE top_recent AS
        SELECT
            page_id,
            ARG_MAX(entity, date) AS entity,
            SUM(views) AS total_views
        FROM read_parquet({uri_list})
        GROUP BY page_id
        ORDER BY total_views DESC
        LIMIT {TOP_DAILY}
    """)

    print("[daily] emitting daily rows for top pages")
    table = con.sql(f"""
        SELECT
            p.date,
            t.page_id,
            t.entity,
            p.views
        FROM read_parquet({uri_list}) p
        INNER JOIN top_recent t ON p.page_id = t.page_id
        ORDER BY t.page_id, p.date
    """).arrow()
    print(f"[daily] done: {len(table):,} rows")
    return table


def run():
    ingest_state = load_state("page_views_ingest")
    last_ingested = ingest_state.get("last_processed_date")
    if not last_ingested:
        print("No ingest data yet — skipping transform.")
        return

    # Only run once ingest has caught up to yesterday. During continuation
    # runs the DAG still calls us, but there's no point transforming a
    # partial window.
    yesterday = (datetime.now() - timedelta(days=1)).date()
    last_date = datetime.strptime(last_ingested, "%Y-%m-%d").date()
    if last_date < yesterday:
        days_behind = (yesterday - last_date).days
        print(f"Ingest is {days_behind} day(s) behind (last={last_date}) — skipping transform until caught up.")
        return

    # Idempotency — don't rebuild if nothing new since last successful run.
    transform_state = load_state("transform")
    if transform_state.get("last_ingested_date") == last_ingested:
        print(f"Transform already up to date at {last_ingested} — skipping.")
        return

    con = connect_duckdb()

    monthly = _build_monthly(con)
    validate(monthly, {
        "not_null": ["month", "page_id", "entity", "views"],
        "min_rows": 1000,
    })
    overwrite(monthly, "wikipedia_top_pages_monthly")
    publish("wikipedia_top_pages_monthly", MONTHLY_METADATA)

    daily = _build_daily_recent(con, last_ingested)
    validate(daily, {
        "not_null": ["date", "page_id", "entity", "views"],
        "min_rows": 1000,
    })
    overwrite(daily, "wikipedia_top_pages_daily_recent")
    publish("wikipedia_top_pages_daily_recent", DAILY_METADATA)

    save_state("transform", {"last_ingested_date": last_ingested})
    print(f"Transform complete at last_ingested_date={last_ingested}")


NODES = {
    run: [ingest_run],
}


if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    run()
