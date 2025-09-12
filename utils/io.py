import os
import json
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import logging
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError, CommitFailedException
from . import debug
from .environment import get_data_dir

logger = logging.getLogger(__name__)

# Storage backend singleton
_storage_backend = None


class LocalStorage:
    """Simple Parquet storage for local development (no Iceberg dependencies)"""
    
    def __init__(self):
        self.base_path = Path(get_data_dir())
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorage initialized at {self.base_path}")
    
    def upload_data(self, data: pa.Table, dataset_name: str) -> str:
        if len(data) == 0:
            logger.warning(f"No data to upload for {dataset_name}")
            return ""
        
        # Use dataset_name as-is (should already include prefix from caller)
        table_name = dataset_name
        
        # Simple append to parquet files
        table_path = self.base_path / table_name
        table_path.mkdir(parents=True, exist_ok=True)
        
        # Generate unique filename based on timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        file_path = table_path / f"{timestamp}.parquet"
        
        pq.write_table(data, file_path)
        logger.info(f"Saved {len(data)} rows to {file_path}")
        
        return str(file_path)
    
    def load_asset(self, connector: str, asset_name: str, run_id: str = None) -> pa.Table:
        _ = run_id  # Not used
        # Use asset_name directly (should already include prefix)
        table_path = self.base_path / asset_name
        
        if not table_path.exists():
            raise FileNotFoundError(f"No data found at {table_path}")
        
        # Read all parquet files in the directory
        parquet_files = list(table_path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {table_path}")
        
        # Read and concatenate all files
        tables = [pq.read_table(f) for f in parquet_files]
        return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


class SubsetsStorage:
    """Iceberg catalog storage for Subsets platform"""
    
    def __init__(self):
        self.catalog = RestCatalog(
            name="subsets",
            uri=os.environ['SUBSETS_CATALOG_URL'],
            token=os.environ['SUBSETS_API_KEY'],
            warehouse=os.environ['SUBSETS_WAREHOUSE']
        )
        logger.info(f"SubsetsStorage initialized with REST catalog")
    
    def upload_data(self, data: pa.Table, dataset_name: str) -> str:
        if len(data) == 0:
            logger.warning(f"No data to upload for {dataset_name}")
            return ""
        
        # Use dataset_name as the table name (should already include prefix from caller)
        table_name = dataset_name
        
        # Get connector name for metadata
        connector = os.environ['CONNECTOR_NAME']
        
        # Use tuple format to separate namespace and table name
        table_identifier = ("subsets", table_name)
             
        # Check if table exists, create if not
        try:
            table = self.catalog.load_table(table_identifier)
            logger.info(f"Found existing table: subsets.{table_name}")
        except NoSuchTableError:
            logger.info(f"Creating new table: subsets.{table_name}")
            table = self.catalog.create_table(
                identifier=table_identifier,
                schema=data.schema,
                properties={'workspace.connector': connector}
            )
        
        # Append data to the table
        try:
            table.append(data)
            logger.info(f"Appended {len(data)} rows to subsets.{table_name}")
        except CommitFailedException as e:
            if "DataInvalid" in str(e):
                if os.environ.get('CONTINUE_ON_SNAPSHOT_ERROR', 'false').lower() == 'true':
                    logger.debug(f"Snapshot mismatch for {table_name}, continuing (data likely already exists)")
                else:
                    raise
            else:
                raise
        
        return f"subsets.{table_name}"
    
    def load_asset(self, connector: str, asset_name: str, run_id: str = None) -> pa.Table:
        _ = run_id  # Not used with Iceberg
        # Use asset_name directly (should already include prefix)
        table_identifier = ("subsets", asset_name)
        
        try:
            table = self.catalog.load_table(table_identifier)
            # Read all data from the table
            df = table.scan().to_pandas()
            return pa.Table.from_pandas(df)
        except NoSuchTableError:
            raise FileNotFoundError(f"No table found: subsets.{table_name}")


def _get_storage():
    """Get or create the storage backend based on CATALOG_TYPE"""
    global _storage_backend
    
    if _storage_backend is None:
        catalog_type = os.environ['CATALOG_TYPE']
        
        if catalog_type == 'local':
            _storage_backend = LocalStorage()
        elif catalog_type == 'subsets':
            _storage_backend = SubsetsStorage()
        else:
            raise ValueError(f"Unknown CATALOG_TYPE: {catalog_type}. Use 'local' or 'subsets'")
    
    return _storage_backend


# Public API functions - thin wrappers around storage backend
def upload_data(data: pa.Table, dataset_name: str) -> str:
    """Upload data to configured storage backend
    
    Args:
        data: The data to upload as a PyArrow table
        dataset_name: Logical dataset name (e.g., "page_views")
    
    Returns:
        str: The storage path where data was saved
    """
    # Print upload info
    size_mb = round(data.nbytes / 1024 / 1024, 2)
    columns = ', '.join([f.name for f in data.schema])
    print(f"Uploading {dataset_name}: {len(data)} rows, {len(data.schema)} cols ({columns}), {size_mb} MB")
    
    # Upload data
    storage = _get_storage()
    key = storage.upload_data(data, dataset_name)
    
    # Log data output
    schema_info = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in data.schema
    ]
    
    metrics = {}
    
    debug.log_data_output(
        dataset_name=dataset_name,
        row_count=len(data),
        column_count=len(data.schema),
        size_bytes=data.nbytes,
        storage_path=key,
        schema=schema_info,
        metrics=metrics
    )
    
    return key


def load_state(asset: str) -> dict:
    """Load state for an asset from local filesystem.
    
    State is always stored locally to enable version control via Git commits
    in GitHub Actions, providing transparency and audit trail.
    """
    catalog_type = os.environ['CATALOG_TYPE']
    state_file = Path(".state") / catalog_type / f"{asset}.json"
    
    if state_file.exists():
        with open(state_file, 'r') as f:
            return json.load(f)
    return {}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset to local filesystem.
    
    State is always stored locally to enable version control via Git commits
    in GitHub Actions, providing transparency and audit trail.
    """
    # Load old state for comparison (for debug logging)
    old_state = load_state(asset)
    
    # Add metadata to state
    state_data = state_data.copy()
    state_data['_metadata'] = {
        'updated_at': datetime.now().isoformat(),
        'run_id': os.environ.get('RUN_ID', 'unknown'),
        'connector': os.environ.get('CONNECTOR_NAME', 'unknown')
    }
    
    # Save state
    catalog_type = os.environ['CATALOG_TYPE']
    state_dir = Path(".state") / catalog_type
    state_dir.mkdir(parents=True, exist_ok=True)
    
    state_file = state_dir / f"{asset}.json"
    with open(state_file, 'w') as f:
        json.dump(state_data, f, indent=2)
    
    # Log state change
    debug.log_state_change(asset, old_state, state_data)
    
    return str(state_file)


def load_asset(connector: str, asset_name: str, run_id: str = None) -> pa.Table:
    """Load a previously saved asset directly from storage for debugging/development
    
    This allows loading assets from previous runs to avoid re-fetching data
    during development. Useful when debugging later stages of a pipeline.
    
    Args:
        connector: The connector name (e.g., 'world-development-indicators')
        asset_name: The dataset/asset name (e.g., 'indicators', 'series')
        run_id: Optional specific run ID to load. If None, loads the most recent.
    
    Returns:
        pa.Table: The loaded PyArrow table
    
    Raises:
        FileNotFoundError: If no asset data found
    """
    return _get_storage().load_asset(connector, asset_name, run_id)


def publish_to_subsets(dataset_name: str = None, metadata: dict = None) -> None:
    """Publish dataset metadata to Subsets platform
    
    After data has been uploaded to Iceberg via upload_data(), this function
    publishes the dataset metadata to make it discoverable on the Subsets platform.
    
    Args:
        dataset_name: Dataset name 
        metadata: Dictionary with dataset metadata:
            - title: Human-readable title
            - description: Dataset description
            - columns: Dict of column_name -> description mappings
    
    Example:
        metadata = {
            "title": "ECB Foreign Exchange Rates",
            "description": "Daily exchange rates from ECB",
            "columns": {
                "date": "The date of observation",
                "USD": "US Dollar to Euro exchange rate",
                "GBP": "British Pound to Euro exchange rate"
            }
        }
        publish_to_subsets(dataset_name="forex_rates", metadata=metadata)
    """
    if not metadata:
        logger.warning("No metadata provided for publishing")
        return
    
    api_key = os.environ.get("SUBSETS_API_KEY")
    if not api_key:
        logger.warning("SUBSETS_API_KEY not set. Skipping publish.")
        return
    
    api_url = os.environ.get("SUBSETS_API_URL", "https://api.subsets.com")
    
    # Determine table name
    if not dataset_name:
        raise ValueError("dataset_name must be provided")
    
    connector = os.environ['CONNECTOR_NAME']
    table_name = f"subsets.{connector}_{dataset_name}"
    
    # Build publish request
    publish_request = {
        "id": table_name,
        "title": metadata.get("title", table_name),
        "description": metadata.get("description", ""),
        "columns": metadata.get("columns", {})
    }
    
    # Make API request to publish
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(
            f"{api_url}/datasets/publish",
            headers=headers,
            json=publish_request,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Dataset published successfully: {table_name}")
            logger.info(f"  Status: {result.get('status')}")
            logger.info(f"  Published at: {result.get('published_at')}")
        elif response.status_code == 409:
            logger.info(f"Dataset {table_name} is already published")
        elif response.status_code == 404:
            logger.error(f"Dataset {table_name} not found in catalog. Ensure data is uploaded first.")
        else:
            logger.error(f"Failed to publish dataset: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error publishing dataset: {e}")