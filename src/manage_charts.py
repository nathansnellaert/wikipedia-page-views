from pathlib import Path
import os
from google.cloud import bigquery
from typing import Dict, List
import requests
import json
from datetime import datetime
import argparse
import gzip

SUBSETS_API_URL = os.getenv('SUBSETS_API_URL', 'https://api.subsets.io')
SUBSETS_API_KEY = os.getenv('SUBSETS_API_KEY')
DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
ICON_URL = "https://storage.googleapis.com/subsets-public-assets/source_logos/wikipedia.png"
CHUNK_SIZE = 1000

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

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
    
    parameters = [bigquery.ArrayQueryParameter("entities", "STRING", entities)]
    if start_date:
        parameters.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    
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

def compress_json(data: Dict) -> bytes:
    return gzip.compress(json.dumps(data).encode('utf-8'))

def create_charts(configs: List[Dict]) -> List[str]:
    compressed_data = compress_json(configs)
    response = requests.post(
        f"{SUBSETS_API_URL}/chart",
        data=compressed_data,
        headers={
            "X-API-Key": SUBSETS_API_KEY,
            "Content-Encoding": "gzip",
            "Content-Type": "application/json"
        }
    )
    response.raise_for_status()
    return response.json()["chart_ids"]

def update_chart_data(updates: Dict) -> None:
    compressed_data = compress_json(updates)
    response = requests.put(
        f"{SUBSETS_API_URL}/chart/data",
        data=compressed_data,
        headers={
            "X-API-Key": SUBSETS_API_KEY,
            "Content-Encoding": "gzip",
            "Content-Type": "application/json"
        }
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
    for entity_chunk in chunk_list(new_entities, CHUNK_SIZE):
        if entity_chunk:
            print(f"Fetching data for chunk of {len(entity_chunk)} new entities...")
            new_entity_data = get_bulk_weekly_data(entity_chunk)
            
            for entity in entity_chunk:
                if entity in new_entity_data and new_entity_data[entity]:
                    config = generate_chart_config(entity)
                    config['data'] = new_entity_data[entity]
                    new_charts.append(config)
    
    updates = {}
    for last_update, entities in entities_by_date.items():
        for entity_chunk in chunk_list(entities, CHUNK_SIZE):
            if entity_chunk:
                print(f"Fetching updates for chunk of {len(entity_chunk)} entities after {last_update}...")
                updated_data = get_bulk_weekly_data(entity_chunk, last_update)
                
                for entity in entity_chunk:
                    if entity in updated_data and updated_data[entity]:
                        updates[metadata[entity]['subsets_id']] = {
                            "create": updated_data[entity]
                        }
                        metadata[entity]['last_update'] = updated_data[entity][-1][0]
    
    for chart_chunk in chunk_list(new_charts, CHUNK_SIZE):
        if chart_chunk:
            print(f"Creating chunk of {len(chart_chunk)} new charts...")
            chart_ids = create_charts(chart_chunk)
            
            for entity, chart_id in zip(
                [chart['title'].split(': ')[1] for chart in chart_chunk],
                chart_ids
            ):
                metadata[entity] = {
                    'subsets_id': chart_id,
                    'last_update': next(
                        chart['data'][-1][0] 
                        for chart in chart_chunk 
                        if chart['title'].split(': ')[1] == entity
                    )
                }
    
    update_chunks = chunk_list(list(updates.items()), CHUNK_SIZE)
    for update_chunk in update_chunks:
        if update_chunk:
            print(f"Updating chunk of {len(update_chunk)} existing charts...")
            update_chart_data(dict(update_chunk))
    
    save_chart_metadata(metadata, metadata_file)
    print("Chart management completed successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manage Wikipedia pageview charts')
    parser.add_argument('--top-n', type=int, default=10000,
                      help='Number of top pages to track (default: 10000)')
    args = parser.parse_args()
    main(args.top_n)