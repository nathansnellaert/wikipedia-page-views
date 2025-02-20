# todo add multi-locale support
from pathlib import Path
import os
import pandas as pd
from google.cloud import bigquery
from subsetsio import SubsetsClient
import unicodedata
import argparse
import json
from datetime import datetime, timedelta

SUBSETS_API_KEY = os.getenv('SUBSETS_API_KEY')
DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
PROJECT_ID = os.environ['GOOGLE_CLOUD_PROJECT']
ICON_URL = 'https://storage.googleapis.com/subsets-public-assets/source_logos/wikimedia.png'

def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize('NFKD', text)
    return normalized.encode('ASCII', 'ignore').decode('ASCII')

def get_last_available_month() -> str:
    query = f"""
    SELECT FORMAT_DATE('%Y-%m', MAX(DATE_TRUNC(PARSE_DATE('%Y%m%d', partition_id), MONTH))) as latest_month
    FROM `{PROJECT_ID}.datasets.INFORMATION_SCHEMA.PARTITIONS` 
    WHERE table_name = 'wikipedia_daily_pageviews_nl'
    """
    client = bigquery.Client()
    return next(iter(client.query(query))).latest_month

def get_top_pages(limit: int) -> pd.DataFrame:
    latest_month = get_last_available_month()
    query = f"""
    WITH monthly_data AS (
        SELECT entity, page_id, 
               SUM(views_sum) as monthly_views,
               ROW_NUMBER() OVER (ORDER BY SUM(views_sum) DESC) as rank
        FROM `{PROJECT_ID}.datasets.wikipedia_daily_pageviews_nl`
        WHERE FORMAT_DATE('%Y-%m', date) = @latest_month
            AND NOT STARTS_WITH(entity, 'List')
            AND LENGTH(entity) > 2
        GROUP BY entity, page_id
    )
    SELECT entity, CAST(page_id as STRING) as page_id
    FROM monthly_data
    WHERE rank <= @limit
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('limit', 'INT64', limit),
            bigquery.ScalarQueryParameter('latest_month', 'STRING', latest_month)
        ])
    return client.query(query, job_config=job_config).to_dataframe()

def get_bulk_monthly_data(entities: list, start_month: str = None, end_month: str = None) -> dict:
    date_filter = "TRUE"
    query_params = [bigquery.ArrayQueryParameter('entities', 'STRING', entities)]
    
    if start_month:
        date_filter = f"FORMAT_DATE('%Y-%m', date) >= @start_month AND FORMAT_DATE('%Y-%m', date) <= @end_month"
        query_params.extend([
            bigquery.ScalarQueryParameter('start_month', 'STRING', start_month),
            bigquery.ScalarQueryParameter('end_month', 'STRING', end_month)
        ])

    query = f"""
    SELECT 
        FORMAT_DATE('%Y-%m', DATE_TRUNC(date, MONTH)) as month,
        entity,
        SUM(views_sum) as monthly_views
    FROM `{PROJECT_ID}.datasets.wikipedia_daily_pageviews_nl`
    WHERE entity IN UNNEST(@entities)
        AND {date_filter}
    GROUP BY month, entity
    ORDER BY month
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    df = client.query(query, job_config=job_config).to_dataframe()
    
    # Group by entity and month
    result = {}
    for entity, group in df.groupby('entity'):
        monthly_sums = group.set_index('month')['monthly_views']
        result[entity] = [[m, int(v)] for m, v in monthly_sums.items()]
    
    return result

def generate_chart_config(entity: str, data: list) -> dict:
    normalized_entity = normalize_text(entity.replace('_', ' '))
    return {
        'data': data,
        'metadata': {
            'type': 'line',
            'title': f'Wikipedia Popularity of {normalized_entity}',
            'icon': ICON_URL,
            'subtitle': 'Monthly page views on English Wikipedia',
            'description': f'Monthly page views for the Wikipedia article on {normalized_entity}',
            'dataset_configs': [{
                'label': f'{normalized_entity} page views',
                'color': '#2563eb',
            }],
        },
        "tags": {"source": "wikipedia-page-views"}
    }

def read_last_update_month(last_update_file: Path) -> str:
    if last_update_file.exists():
        with open(last_update_file) as f:
            return f.read().strip()
    return None

def write_last_update_month(last_update_file: Path, month: str):
    with open(last_update_file, 'w') as f:
        f.write(month)

def main(top_n: int, regenerate_top: bool):
    if not SUBSETS_API_KEY:
        raise ValueError('SUBSETS_API_KEY environment variable must be set')
    
    DATA_DIR.mkdir(exist_ok=True)
    tracking_file = DATA_DIR / 'entities_to_track.csv'
    chart_mapping_file = DATA_DIR / 'chart_mapping.json'
    last_update_file = DATA_DIR / 'last_update.txt'
    
    # Load or generate tracking data
    if tracking_file.exists() and not regenerate_top:
        tracking_df = pd.read_csv(tracking_file)
    else:
        tracking_df = get_top_pages(top_n)
        tracking_df.to_csv(tracking_file, index=False)
        
    # Process new and existing entities
    subsets = SubsetsClient(api_key=SUBSETS_API_KEY)
    latest_available_month = get_last_available_month()
    last_update_month = read_last_update_month(last_update_file)
    
    if last_update_month == latest_available_month:
        print('No new data available')
        return
    
    charts_exist = chart_mapping_file.exists()
    if charts_exist:
        with open(chart_mapping_file) as f:
            chart_mapping = json.load(f)

        new_data = get_bulk_monthly_data(tracking_df.entity.tolist(), start_month=last_update_month, end_month=latest_available_month)        
        data_updates = {}
        # return as dictionary, where key is the chart_id on subsets, and value is the data
        for entity, data in new_data.items():
            data_updates[chart_mapping[entity]] = data

        subsets.add_data_rows(data_updates)
    else:
        all_data = get_bulk_monthly_data(tracking_df.entity.tolist(), end_month=latest_available_month)
        charts = []
        for entity, data in all_data.items():
            chart = generate_chart_config(entity, data)
            if len(chart['data']) > 10:
                charts.append(chart)
    
        chart_ids = subsets.create(charts)
        chart_mapping = dict(zip(all_data.keys(), chart_ids))
        with open(chart_mapping_file, 'w') as f:
            json.dump(chart_mapping, f)

    # set last update month to latest available month
    write_last_update_month(last_update_file, latest_available_month)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage Wikipedia pageview charts')
    parser.add_argument('--top-n', type=int, default=10000,
                      help='Number of top pages to track (default: 10000)')
    parser.add_argument('--regenerate-top', action='store_true',
                      help='Regenerate and merge new top pages')
    args = parser.parse_args()
    main(args.top_n, args.regenerate_top)