"""Ingest Wikipedia page view data from Wikimedia dumps."""
import os
import bz2
import time
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import httpx
import pyarrow as pa
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from connector_utils import asset_id_for_date
from subsets_utils import get, load_state, save_state, save_raw_parquet, configure_http

GH_ACTIONS_MAX_RUN_SECONDS = 5.8 * 60 * 60
PARALLELISM_COUNT = 1
STATE_KEY = "page_views_ingest"
DOWNLOAD_TIMEOUT = 600  # seconds — dump files are hundreds of MB

configure_http(timeout=DOWNLOAD_TIMEOUT)


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 429 (rate limit) and 5xx (server errors)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
        return True
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=10, min=10, max=300),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_pageviews_for_date(date: datetime) -> None:
    """Fetch page views for a date and save as parquet."""
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
        'views': pa.array(views, type=pa.int64()),
    })
    save_raw_parquet(table, asset_id_for_date(date))


def load_completed_dates() -> set[str]:
    """Read completed dates from state. Trusted source of truth across runs."""
    return set(load_state(STATE_KEY).get('completed_dates', []))


def pending_dates(start_date: datetime, end_date: datetime, completed: set[str]) -> list[datetime]:
    pending = []
    d = start_date
    while d <= end_date:
        if d.strftime('%Y-%m-%d') not in completed:
            pending.append(d)
        d += timedelta(days=1)
    return pending


def run() -> bool:
    """Fetch Wikipedia page views and save raw data.

    Returns True if more work remains and the workflow should re-trigger.
    """
    start_time = time.time()

    default_start = os.environ.get('DATA_COLLECTION_START_DATE', '2016-01-01')
    start_date = datetime.strptime(default_start, '%Y-%m-%d')
    end_date = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    completed = load_completed_dates()
    print(f"  {len(completed)} dates already ingested")

    todo = pending_dates(start_date, end_date, completed)
    if not todo:
        print("  Already caught up - no new data to process")
        return False

    print(f"  Processing {len(todo)} pending dates ({todo[0].strftime('%Y-%m-%d')}..{todo[-1].strftime('%Y-%m-%d')})")

    with ThreadPoolExecutor(max_workers=PARALLELISM_COUNT) as executor:
        for i in range(0, len(todo), PARALLELISM_COUNT):
            elapsed = time.time() - start_time
            if elapsed >= GH_ACTIONS_MAX_RUN_SECONDS:
                print(f"\n  Time budget exhausted ({elapsed/3600:.1f}h)")
                save_state(STATE_KEY, {
                    'completed_dates': sorted(completed),
                    'last_processed_date': sorted(completed)[-1] if completed else None,
                })
                return True

            batch = todo[i:i + PARALLELISM_COUNT]
            print(f"  [{i + 1}-{i + len(batch)}/{len(todo)}] {batch[0].strftime('%Y-%m-%d')}..{batch[-1].strftime('%Y-%m-%d')}")

            futures = {executor.submit(fetch_pageviews_for_date, d): d for d in batch}
            for future in as_completed(futures):
                d = futures[future]
                future.result()
                completed.add(d.strftime('%Y-%m-%d'))
                print(f"  -> Saved {asset_id_for_date(d)}")

            save_state(STATE_KEY, {
                'completed_dates': sorted(completed),
                'last_processed_date': sorted(completed)[-1],
            })

            # Small delay between batches to respect Wikimedia rate limits
            time.sleep(2)

    print(f"\n  Done — {len(todo)} dates processed this run")
    return False


NODES = {
    run: [],
}

if __name__ == "__main__":
    from subsets_utils import validate_environment
    validate_environment()
    run()
