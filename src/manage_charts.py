from pathlib import Path
import os
from google.cloud import bigquery
from typing import Dict, List
import requests
import json
from datetime import datetime
import argparse

SUBSETS_API_URL = os.getenv('SUBSETS_API_URL', 'https://api.subsets.io')
SUBSETS_API_KEY = os.getenv('SUBSETS_API_KEY')
DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
ICON_URL = "https://storage.googleapis.com/subsets-public-assets/source_logos/wikipedia.png"

def get_top_pages(limit: int) -> List[str]:
    query = """
    WITH latest_partition AS (
      SELECT MAX(partition_id) as latest_date
      FROM `{}.datasets.INFORMATION_SCHEMA.PARTITIONS` 
      WHERE table_name = 'wikipedia_daily_pageviews'
    ),
    latest_data AS (
        SELECT entity, page_id, views_sum,
               ROW_NUMBER() OVER (ORDER BY views_sum DESC) as rank
        FROM `{}.datasets.wikipedia_daily_pageviews`
        WHERE date = PARSE_DATE('%Y%m%d', (SELECT latest_date FROM latest_partition))
    )
    SELECT entity, page_id
    FROM latest_data
    WHERE rank <= @limit
    """.format(PROJECT_ID, PROJECT_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit)
        ]
    )
    
    client = bigquery.Client()
    return [row.entity for row in client.query(query, job_config=job_config)]

def get_bulk_weekly_data(entities: List[str], start_date: str = None) -> Dict[str, List[List]]:
    query = """
    WITH weeks AS (
        SELECT 
            DATE_TRUNC(date, WEEK) as week_start,
            entity,
            SUM(views_sum) as weekly_views
        FROM `{}.datasets.wikipedia_daily_pageviews`
        WHERE entity IN UNNEST(@entities)
            {}
            AND DATE_TRUNC(date, WEEK) < DATE_TRUNC(CURRENT_DATE(), WEEK)
        GROUP BY 1, 2
    )
    SELECT week_start, entity, weekly_views
    FROM weeks
    ORDER BY week_start
    """.format(
        PROJECT_ID,
        "AND DATE_TRUNC(date, WEEK) > @start_date" if start_date else ""
    )
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("entities", "STRING", entities),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date) if start_date else None
        ]
    )
    
    client = bigquery.Client()
    result = client.query(query, job_config=job_config)
    
    entity_data = {}
    for row in result:
        if row.entity not in entity_data:
            entity_data[row.entity] = []
        entity_data[row.entity].append([row.week_start.strftime('%Y-%m-%d'), row.weekly_views])
    
    return entity_data

def load_chart_metadata(metadata_file: Path) -> Dict:
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            return json.load(f)
    return {}

def save_chart_metadata(metadata: Dict, metadata_file: Path):
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)

def generate_chart_config(entity: str) -> Dict:
    return {
        "type": "line",
        "title": f"Wikipedia Weekly Views: {entity}",
        "icon": ICON_URL,
        "subtitle": "Weekly pageview totals",
        "description": f"Weekly Wikipedia pageview statistics for {entity}",
        "dataset_configs": [{
            "label": "Views",
            "line_style": "solid",
            "color": "#2563eb",
            "point_size": 4
        }],
        "x_axis": {
            "label": "Week Starting",
        },
        "y_axis": {
            "label": "Weekly Views",
        },
        "show_legend": True,
        "background_color": "#FFFFFF"
    }

def create_charts(configs: List[Dict]) -> List[str]:
    response = requests.post(
        f"{SUBSETS_API_URL}/chart",
        json=configs,
        headers={"X-API-Key": SUBSETS_API_KEY}
    )
    response.raise_for_status()
    return response.json()["chart_ids"]

def update_chart_data(updates: Dict) -> None:
    response = requests.put(
        f"{SUBSETS_API_URL}/chart/data",
        json=updates,
        headers={"X-API-Key": SUBSETS_API_KEY}
    )
    response.raise_for_status()

def main(top_n: int):
    if not SUBSETS_API_KEY:
        raise ValueError("SUBSETS_API_KEY environment variable must be set")

    DATA_DIR.mkdir(exist_ok=True)
    metadata_file = DATA_DIR / 'chart_metadata.json'
    metadata = load_chart_metadata(metadata_file)
    
    print(f"Fetching top {top_n} pages from BigQuery...")
    top_entities = get_top_pages(top_n)
    
    new_entities = []
    entities_by_date = {}
    
    for entity in top_entities:
        if entity in metadata:
            last_update = metadata[entity]['last_update']
            if last_update not in entities_by_date:
                entities_by_date[last_update] = []
            entities_by_date[last_update].append(entity)
        else:
            new_entities.append(entity)
    
    new_charts = []
    if new_entities:
        print(f"Fetching data for {len(new_entities)} new entities...")
        new_entity_data = get_bulk_weekly_data(new_entities)
        
        for entity in new_entities:
            if entity in new_entity_data and new_entity_data[entity]:
                config = generate_chart_config(entity)
                config['data'] = new_entity_data[entity]
                new_charts.append(config)
    
    updates = {}
    for last_update, entities in entities_by_date.items():
        print(f"Fetching updates for {len(entities)} entities after {last_update}...")
        updated_data = get_bulk_weekly_data(entities, last_update)
        
        for entity in entities:
            if entity in updated_data and updated_data[entity]:
                updates[metadata[entity]['subsets_id']] = {
                    "create": updated_data[entity]
                }
                metadata[entity]['last_update'] = updated_data[entity][-1][0]
    
    if new_charts:
        print(f"Creating {len(new_charts)} new charts...")
        chart_ids = create_charts(new_charts)
        
        for entity, chart_id in zip(new_entities, chart_ids):
            metadata[entity] = {
                'subsets_id': chart_id,
                'last_update': new_charts[0]['data'][-1][0]
            }
    
    if updates:
        print(f"Updating {len(updates)} existing charts...")
        update_chart_data(updates)
    
    save_chart_metadata(metadata, metadata_file)
    print("Chart management completed successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manage Wikipedia pageview charts')
    parser.add_argument('--top-n', type=int, default=10000,
                      help='Number of top pages to track (default: 10000)')
    args = parser.parse_args()
    main(args.top_n)