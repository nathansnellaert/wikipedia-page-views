
"""Ingest Wikipedia page view data."""
import os

import bz2
import time
import io
from datetime import datetime, timedelta
import pyarrow as pa
from subsets_utils import get, load_state, save_state, save_raw_parquet

GH_ACTIONS_MAX_RUN_SECONDS = 5.8 * 60 * 60

def fetch_pageviews_for_date(date: datetime) -> int:
    """Fetch page views for a specific date and write directly to Parquet.

    Returns:
        Number of rows written
    """
    url = f"https://dumps.wikimedia.org/other/pageview_complete/{date.year}/{date.year}-{date.month:02d}/pageviews-{date.year}{date.month:02d}{date.day:02d}-user.bz2"

    print(f"    Fetching {url}")
    response = get(url)
    response.raise_for_status()

    date_str = date.strftime('%Y-%m-%d')
    dates, entities, page_ids, views = [], [], [], []

    with bz2.open(io.BytesIO(response.content), mode='rt') as bz2_file:
        for line in bz2_file:
            split = line.strip().split()
            if len(split) != 6:
                continue

            project, entity, page_id, _, view_count, _ = split

            if project == 'en.wikipedia' and page_id != 'null':
                dates.append(date_str)
                entities.append(entity)
                page_ids.append(page_id)
                views.append(int(view_count))

    if not dates:
        raise ValueError(f"No data found for date {date_str}")

    table = pa.table({
        'date': pa.array(dates, type=pa.string()),
        'entity': pa.array(entities, type=pa.string()),
        'page_id': pa.array(page_ids, type=pa.string()),
        'views': pa.array(views, type=pa.int64())
    })

    save_raw_parquet(table, f"page_views_{date.strftime('%Y%m%d')}")
    return len(dates)


def run() -> bool:
    """Fetch Wikipedia page views and save raw data.

    Returns:
        bool: True if there's more work to do (should re-trigger), False if caught up
    """
    start_time = time.time()

    state = load_state("page_views_ingest")
    last_processed_date = state.get('last_processed_date')

    default_start = os.environ.get('DATA_COLLECTION_START_DATE', '2016-01-01')

    if last_processed_date:
        start_date = datetime.strptime(last_processed_date, '%Y-%m-%d') + timedelta(days=1)
    else:
        start_date = datetime.strptime(default_start, '%Y-%m-%d')

    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    if start_date > end_date:
        print("  Already caught up - no new data to process")
        return False

    days_remaining = (end_date - start_date).days + 1
    print(f"  Processing {days_remaining} days from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    current_date = start_date
    days_processed = 0

    while current_date <= end_date:
        elapsed = time.time() - start_time
        if elapsed >= GH_ACTIONS_MAX_RUN_SECONDS:
            print(f"\n  Time budget exhausted after {days_processed} days ({elapsed/3600:.1f} hours)")
            return True

        try:
            print(f"  [{days_processed + 1}/{days_remaining}] {current_date.strftime('%Y-%m-%d')}")
            fetch_pageviews_for_date(current_date)

            save_state("page_views_ingest", {
                'last_processed_date': current_date.strftime('%Y-%m-%d')
            })
            days_processed += 1

        except ValueError as e:
            print(f"    Error: {e}")

        current_date += timedelta(days=1)

    print(f"\n  Completed! Processed {days_processed} days")
    return False
