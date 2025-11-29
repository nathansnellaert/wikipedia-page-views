import os
import bz2
import time
from datetime import datetime, timedelta
import pyarrow as pa
from utils import get, load_state, save_state, upload_raw_to_r2

WIKI_LOCALES = {
    'en': 'en.wikipedia',
    'nl': 'nl.wikipedia'
}

# Time budget: 5 hours (leave 1 hour buffer before GitHub's 6 hour limit)
TIME_BUDGET_SECONDS = 5 * 60 * 60


def process_page_views() -> bool:
    """Process Wikipedia page views data.

    Returns:
        bool: True if there's more work to do (should re-trigger), False if caught up
    """
    start_time = time.time()

    # Load state to track processed dates
    state = load_state("page_views")
    last_processed_date = state.get('last_processed_date')

    # Get start date from environment or default
    default_start = os.environ.get('DATA_COLLECTION_START_DATE', '2016-01-01')

    if last_processed_date:
        start_date = datetime.strptime(last_processed_date, '%Y-%m-%d') + timedelta(days=1)
    else:
        start_date = datetime.strptime(default_start, '%Y-%m-%d')

    # Only fetch data up to yesterday (completed days)
    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    if start_date > end_date:
        print("Already caught up - no new data to process")
        return False

    days_remaining = (end_date - start_date).days + 1
    print(f"Processing {days_remaining} days from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Process one day at a time
    current_date = start_date
    days_processed = 0

    while current_date <= end_date:
        # Check time budget before starting a new day
        elapsed = time.time() - start_time
        if elapsed >= TIME_BUDGET_SECONDS:
            print(f"\nTime budget exhausted after {days_processed} days ({elapsed/3600:.1f} hours)")
            print(f"Remaining: {(end_date - current_date).days + 1} days")
            return True  # Signal that we need to continue

        try:
            print(f"Processing {current_date.strftime('%Y-%m-%d')} ({days_processed + 1}/{days_remaining})")
            date_data = fetch_pageviews_for_date(current_date)

            # Create table for this day's data
            day_table = pa.Table.from_arrays([
                pa.array([row['date'].date() for row in date_data], type=pa.date32()),
                pa.array([row['locale'] for row in date_data]),
                pa.array([row['entity'] for row in date_data]),
                pa.array([row['page_id'] for row in date_data]),
                pa.array([row['views'] for row in date_data])
            ], names=['date', 'locale', 'entity', 'page_id', 'views'])

            # Group by date, locale, entity, page_id and sum views
            grouped_table = day_table.group_by(['date', 'locale', 'entity', 'page_id']).aggregate([('views', 'sum')])

            # Upload data for this date to R2
            upload_raw_to_r2(grouped_table, f"wikipedia_page_views/{current_date.strftime('%Y-%m-%d')}.parquet")

            # Update state after successful upload
            save_state("page_views", {'last_processed_date': current_date.strftime('%Y-%m-%d')})
            days_processed += 1

        except ValueError as e:
            print(f"Error processing {current_date.strftime('%Y-%m-%d')}: {e}")

        current_date += timedelta(days=1)

    print(f"\nCompleted! Processed {days_processed} days in {(time.time() - start_time)/3600:.1f} hours")
    return False  # All caught up
    

def fetch_pageviews_for_date(date: datetime) -> list:
    """Fetch page views for a specific date."""
    url = f"https://dumps.wikimedia.org/other/pageview_complete/{date.year}/{date.year}-{date.month:02d}/pageviews-{date.year}{date.month:02d}{date.day:02d}-user.bz2"
    
    print(f"Fetching {url}")
    response = get(url)
    response.raise_for_status()
    
    data = []
    
    # Process the bz2 compressed data
    import io
    with bz2.open(io.BytesIO(response.content), mode='rt') as bz2_file:
        for line in bz2_file:
            split = line.strip().split()
            if len(split) != 6:
                continue
            
            project, entity, page_id, _, view_count, _ = split
            
            # Check if this project is one we're interested in
            for locale_id, project_name in WIKI_LOCALES.items():
                if project == project_name and page_id != 'null':
                    data.append({
                        'date': date,
                        'locale': locale_id,
                        'entity': entity,
                        'page_id': page_id,
                        'views': int(view_count)
                    })
    
    if not data:
        raise ValueError(f"No data found for date {date.strftime('%Y-%m-%d')}")
    
    return data