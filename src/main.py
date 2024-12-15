import argparse
import requests
import bz2
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
from io import BytesIO
import os

DATASET_ID = "datasets"
TABLE_NAME = "wikipedia_daily_pageviews"
GCS_PREFIX = TABLE_NAME
WEEKLY_TABLE_NAME = "wikipedia_weekly_pageviews"

def setup_views(bq_client, project_id, bucket_name):
    daily_view = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{DATASET_ID}.{TABLE_NAME}`
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://{bucket_name}/{GCS_PREFIX}/*.parquet']
    )
    """
    
    weekly_view = f"""
    CREATE OR REPLACE VIEW `{project_id}.{DATASET_ID}.{WEEKLY_TABLE_NAME}` AS 
    WITH weekly_totals AS (
        SELECT 
            DATE_TRUNC(date, WEEK) as week_start,
            entity,
            page_id,
            SUM(views_sum) as weekly_views
        FROM `{project_id}.{DATASET_ID}.{TABLE_NAME}`
        GROUP BY 1, 2, 3
    )
    SELECT * FROM weekly_totals
    ORDER BY weekly_views DESC
    LIMIT 200000
    """
    
    bq_client.query(daily_view).result()
    bq_client.query(weekly_view).result()

def get_existing_partitions(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{GCS_PREFIX}/"))
    return {blob.name.split("/")[-1].split(".")[0] for blob in blobs}

def fetch_pageviews(date):
    url = f"https://dumps.wikimedia.org/other/pageview_complete/{date.year}/{date.year}-{date.month:02d}/pageviews-{date.year}{date.month:02d}{date.day:02d}-user.bz2"
    
    dates, entities, page_ids, views = [], [], [], []
    
    print(f"Fetching {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with bz2.open(r.raw, mode='rt') as bz2_file:
            for line in bz2_file:
                split = line.split()
                if len(split) != 6:
                    continue
                project, entity, page_id, _, view_count, _ = split
                if project == 'en.wikipedia' and page_id != 'null':
                    dates.append(date.date())
                    entities.append(entity)
                    page_ids.append(page_id)
                    views.append(int(view_count))

    table = pa.Table.from_arrays([
        pa.array(dates, type=pa.date32()),
        pa.array(entities),
        pa.array(page_ids),
        pa.array(views)
    ], names=['date', 'entity', 'page_id', 'views'])

    return table.group_by(['date', 'entity', 'page_id']).aggregate([('views', 'sum')])

def main(start_date):
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    bucket_name = os.environ["BUCKET_NAME"]
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.now()
    all_partitions = [
        (start + timedelta(days=x)).strftime('%Y-%m-%d')
        for x in range((end - start).days + 1)
    ]
    
    existing_partitions = get_existing_partitions(bucket_name)
    remaining_partitions = [p for p in all_partitions if p not in existing_partitions]
    print(f"Found {len(remaining_partitions)} partitions to fetch")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    current_date = datetime.now()
    
    for partition_key in remaining_partitions:
        date = datetime.strptime(partition_key, '%Y-%m-%d')
        days_difference = (current_date - date).days
        
        try:
            table = fetch_pageviews(date)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            blob = bucket.blob(f"{GCS_PREFIX}/{partition_key}.parquet")
            blob.upload_from_file(buffer, content_type='application/octet-stream')
            print(f"Saved {table.num_rows} records")
        except Exception as e:
            if days_difference > 7:
                raise
            print(f"Failed loading {partition_key}. Likely because the data is not available yet.")

    bq_client = bigquery.Client()
    setup_views(bq_client, project_id, bucket_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Wikipedia pageview stats')
    parser.add_argument('--start-date', type=str, required=True,
                      help='Start date (YYYY-MM-DD)')
    args = parser.parse_args()
    main(args.start_date)