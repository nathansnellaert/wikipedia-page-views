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
from typing import Dict, List
from dataclasses import dataclass

DATASET_ID = "datasets"

@dataclass
class WikiLocale:
    table_name: str
    project: str
    gcs_prefix: str

WIKI_LOCALES = {
    'en': WikiLocale(
        table_name='wikipedia_daily_pageviews_en',
        project='en.wikipedia',
        gcs_prefix='wikipedia_daily_pageviews_en'
    ),
    'nl': WikiLocale(
        table_name='wikipedia_daily_pageviews_nl',
        project='nl.wikipedia',
        gcs_prefix='wikipedia_daily_pageviews_nl'
    )
}

def sync_gcs_bq(project_id: str, bucket_name: str, bq_client: bigquery.Client, locale: WikiLocale):
    table_id = f"{project_id}.{DATASET_ID}.{locale.table_name}"
    
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("entity", "STRING"),
        bigquery.SchemaField("page_id", "STRING"),
        bigquery.SchemaField("views_sum", "INTEGER"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"
    )
    table.clustering_fields = ["entity"]
    
    table = bq_client.create_table(table, exists_ok=True)
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET,
    )
    
    load_job = bq_client.load_table_from_uri(
        f"gs://{bucket_name}/{locale.gcs_prefix}/*.parquet",
        table_id,
        job_config=job_config
    )
    load_job.result()

def get_existing_partitions(bucket_name: str, locale: WikiLocale) -> set:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{locale.gcs_prefix}/"))
    return {blob.name.split("/")[-1].split(".")[0] for blob in blobs}

def fetch_pageviews(date: datetime) -> Dict[str, pa.Table]:
    url = f"https://dumps.wikimedia.org/other/pageview_complete/{date.year}/{date.year}-{date.month:02d}/pageviews-{date.year}{date.month:02d}{date.day:02d}-user.bz2"
    
    # Initialize data structures for each locale
    locale_data = {
        locale: {
            'dates': [], 
            'entities': [], 
            'page_ids': [], 
            'views': []
        } 
        for locale in WIKI_LOCALES
    }
    
    print(f"Fetching {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with bz2.open(r.raw, mode='rt') as bz2_file:
            for line in bz2_file:
                split = line.split()
                if len(split) != 6:
                    continue
                project, entity, page_id, _, view_count, _ = split
                
                # Find matching locale
                for locale_id, config in WIKI_LOCALES.items():
                    if project == config.project and page_id != 'null':
                        data = locale_data[locale_id]
                        data['dates'].append(date.date())
                        data['entities'].append(entity)
                        data['page_ids'].append(page_id)
                        data['views'].append(int(view_count))
    
    # Convert to tables
    tables = {}
    for locale_id, data in locale_data.items():
        if not data['dates']:
            raise ValueError(f"No data found for locale {locale_id} on {date}")
            
        table = pa.Table.from_arrays([
            pa.array(data['dates'], type=pa.date32()),
            pa.array(data['entities']),
            pa.array(data['page_ids']),
            pa.array(data['views'])
        ], names=['date', 'entity', 'page_id', 'views'])
        
        tables[locale_id] = table.group_by(['date', 'entity', 'page_id']).aggregate([('views', 'sum')])
    
    return tables

def main(start_date: str):
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    bucket_name = os.environ["BUCKET_NAME"]
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.now()
    all_partitions = [
        (start + timedelta(days=x)).strftime('%Y-%m-%d')
        for x in range((end - start).days + 1)
    ]
    
    # Get existing partitions for each locale
    remaining_partitions_by_locale = {}
    for locale_id, locale_config in WIKI_LOCALES.items():
        existing = get_existing_partitions(bucket_name, locale_config)
        remaining = [p for p in all_partitions if p not in existing]
        remaining_partitions_by_locale[locale_id] = remaining
        print(f"Found {len(remaining)} partitions to fetch for {locale_id}")
    
    # Combine all needed partitions
    all_remaining_partitions = set().union(*remaining_partitions_by_locale.values())
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    current_date = datetime.now()
    bq_client = bigquery.Client()
    
    for partition_key in all_remaining_partitions:
        date = datetime.strptime(partition_key, '%Y-%m-%d')
        days_difference = (current_date - date).days
        
        try:
            tables = fetch_pageviews(date)
            
            # Save tables for each locale
            for locale_id, table in tables.items():
                if partition_key not in remaining_partitions_by_locale[locale_id]:
                    continue
                    
                buffer = BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0)
                
                locale_config = WIKI_LOCALES[locale_id]
                blob = bucket.blob(f"{locale_config.gcs_prefix}/{partition_key}.parquet")
                blob.upload_from_file(buffer, content_type='application/octet-stream')
                print(f"Saved {table.num_rows} records for {locale_id}")
                
        except Exception as e:
            if days_difference > 7:
                raise
            print(f"Failed loading {partition_key}. Likely because the data is not available yet.")
            continue
    
    # Sync each locale to BigQuery
    for locale_config in WIKI_LOCALES.values():
        sync_gcs_bq(project_id, bucket_name, bq_client, locale_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Wikipedia pageview stats')
    parser.add_argument('--start-date', type=str, required=True,
                      help='Start date (YYYY-MM-DD)')
    args = parser.parse_args()
    main(args.start_date)