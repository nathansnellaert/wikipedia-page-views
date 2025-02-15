from pathlib import Path
import os
from google.cloud import bigquery
from typing import Dict, List
import argparse
from subsetsio import SubsetsClient
import json
import unicodedata

SUBSETS_API_KEY = os.getenv('SUBSETS_API_KEY')
DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
PROJECT_ID = os.environ['GOOGLE_CLOUD_PROJECT']
ICON_URL = (
    'https://storage.googleapis.com/subsets-public-assets/source_logos/wikimedia.png'
)

def normalize_text(text: str) -> str:
    """Normalize Unicode characters in text to their closest ASCII representation."""
    # Normalize to NFKD form and encode as ASCII, dropping non-ASCII characters
    normalized = unicodedata.normalize('NFKD', text)
    return normalized.encode('ASCII', 'ignore').decode('ASCII')

def get_last_available_date() -> str:
    query = (
        """
    SELECT MAX(partition_id) as latest_date
    FROM `{}.datasets.INFORMATION_SCHEMA.PARTITIONS` 
    WHERE table_name = 'wikipedia_daily_pageviews'
    """
        .format(PROJECT_ID))
    client = bigquery.Client()
    query_job = client.query(query)
    result = next(iter(query_job))
    return result.latest_date

def get_top_pages(limit: int) -> List[str]:
    latest_date = get_last_available_date()
    query = (
        """
    WITH latest_data AS (
        SELECT entity, page_id, views_sum,
               ROW_NUMBER() OVER (ORDER BY views_sum DESC) as rank
        FROM `{}.datasets.wikipedia_daily_pageviews`
        WHERE date = PARSE_DATE('%Y%m%d', @latest_date)
            AND NOT STARTS_WITH(entity, 'List')
            AND LENGTH(entity) > 2
    )
    SELECT entity, page_id
    FROM latest_data
    WHERE rank <= @limit
    """
        .format(PROJECT_ID))
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('limit', 'INT64', limit),
            bigquery.ScalarQueryParameter('latest_date', 'STRING', latest_date)
        ])
    client = bigquery.Client()
    query_job = client.query(query, job_config=job_config)
    bytes_processed = query_job.total_bytes_processed if query_job.total_bytes_processed else 0
    print(f'Query processed {bytes_processed / 1024 / 1024 / 1024:.2f} GB')
    return [row.entity for row in query_job]

def get_bulk_daily_data(entities: List[str]) -> List[List]:
    query = (
        """
    SELECT date, entity, views_sum
    FROM `{}.datasets.wikipedia_daily_pageviews`
    WHERE entity IN UNNEST(@entities)
        AND date < CURRENT_DATE()
    ORDER BY date
    """
        .format(PROJECT_ID))
    parameters = [bigquery.ArrayQueryParameter('entities', 'STRING', entities)]
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    client = bigquery.Client()
    query_job = client.query(query, job_config=job_config)
    bytes_processed = query_job.total_bytes_processed if query_job.total_bytes_processed else 0
    print(f'Query processed {bytes_processed / 1024 / 1024 / 1024:.2f} GB')
    entity_data = {}
    for row in query_job:
        if row.entity not in entity_data:
            entity_data[row.entity] = []
        entity_data[row.entity].append([row.date.strftime('%Y-%m-%d'), row.views_sum])
    return entity_data

def generate_chart_config(entity: str, data: List[List]) -> Dict:
    normalized_entity = normalize_text(entity.replace('_', ' '))
    
    return {
        'data': data,
        'metadata': {
            'type': 'line',
            'title': f'Wikipedia Popularity of {normalized_entity}',
            'icon': ICON_URL,
            'subtitle': 'Daily page views on English Wikipedia',
            'description': f'Daily page views for the Wikipedia article on {normalized_entity}',
            'dataset_configs': [{
                'label': f'{normalized_entity} page views',
                'color': '#2563eb',
            }],
        },
        "tags": {
            "source": "wikipedia-page-views"
        }
    }

def main(top_n: int, regenerate_top: bool):
    if not SUBSETS_API_KEY:
        raise ValueError('SUBSETS_API_KEY environment variable must be set')
    
    subsets = SubsetsClient(api_key=SUBSETS_API_KEY)
    DATA_DIR.mkdir(exist_ok=True)
    metadata_file = DATA_DIR / 'chart_metadata.json'
    last_update_file = DATA_DIR / 'last_update.txt'
    
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
    
    entities_to_track = set(metadata.keys())
    if not metadata or regenerate_top:
        print(f'Fetching top {top_n} pages from BigQuery...')
        entities_to_track.update(get_top_pages(top_n))
    
    entities_to_track = list(entities_to_track)
    if not entities_to_track:
        print('No entities to track')
        return
    
    latest_date = get_last_available_date()
    last_processed_date = None
    if last_update_file.exists():
        with open(last_update_file, 'r') as f:
            last_processed_date = f.read().strip()
    
    if not last_processed_date or last_processed_date < latest_date:
        new_entities = [e for e in entities_to_track if e not in metadata]
        existing_entities = [e for e in entities_to_track if e in metadata]
        
        if new_entities:
            print(f'Fetching data for {len(new_entities)} new entities...')
            new_entity_data = get_bulk_daily_data(new_entities)
            charts_to_create = []
            
            for entity in new_entities:
                if entity in new_entity_data and new_entity_data[entity]:
                    chart_config = generate_chart_config(entity, new_entity_data[entity])
                    charts_to_create.append(chart_config)
            
            if charts_to_create:
                print(f'Creating {len(charts_to_create)} new charts...')
                chart_ids = subsets.create(charts_to_create)
                for entity, chart_id in zip(new_entities, chart_ids):
                    metadata[entity] = {'chart_id': chart_id}
        
        if existing_entities:
            print(f'Fetching updates for {len(existing_entities)} existing entities...')
            updated_data = get_bulk_daily_data(existing_entities)
            data_updates = {
                metadata[entity]['chart_id']: updated_data[entity]
                for entity in existing_entities
                if entity in updated_data and updated_data[entity]
            }
            
            if data_updates:
                print(f'Updating {len(data_updates)} charts...')
                subsets.add_data_rows(data_updates)
        
        with open(last_update_file, 'w') as f:
            f.write(latest_date)
    else:
        print('No updates needed - already processed latest data')
    
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)
    
    print('Chart management completed successfully')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage Wikipedia pageview charts')
    parser.add_argument('--top-n', type=int, default=10000,
                      help='Number of top pages to track (default: 10000)')
    parser.add_argument('--regenerate-top', action='store_true',
                      help='Regenerate and merge new top pages')
    args = parser.parse_args()
    main(args.top_n, args.regenerate_top)