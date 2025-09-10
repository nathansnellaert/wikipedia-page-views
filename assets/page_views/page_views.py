import os
import bz2
from datetime import datetime, timedelta
import pyarrow as pa
from utils import get, load_state, save_state, upload_data

WIKI_LOCALES = {
    'en': 'en.wikipedia',
    'nl': 'nl.wikipedia'
}

def process_page_views() -> pa.Table:
    """Process Wikipedia page views data."""
    
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
    
    # Process one day at a time to avoid memory issues
    current_date = start_date
    while current_date <= end_date:
        try:
            print(f"Processing data for {current_date.strftime('%Y-%m-%d')}")
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
            
            # Upload data for this date
            upload_data(grouped_table, "wikipedia_page_views")
            
            # Update state after successful upload
            save_state("page_views", {'last_processed_date': current_date.strftime('%Y-%m-%d')})
            
        except ValueError as e:
            print(f"Error processing {current_date.strftime('%Y-%m-%d')}: {e}")
        
        current_date += timedelta(days=1)
    

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