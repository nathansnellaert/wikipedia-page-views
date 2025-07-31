import os
import json
from datetime import datetime
from pathlib import Path
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import logging
from . import debug
from .environment import get_data_dir

try:
    from subsets_client import SubsetsClient
except ImportError:
    SubsetsClient = None

logger = logging.getLogger(__name__)

# Storage backend singleton
_storage_backend = None


class LocalStorage:
    """Local filesystem storage for development"""
    
    def __init__(self):
        self.base_path = Path(get_data_dir())
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorage initialized at {self.base_path}")
    
    def upload_data(self, data: pa.Table, dataset_name: str, partition: str = None) -> str:
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        # Build path with optional partition
        if partition:
            path = self.base_path / connector / dataset_name / partition
        else:
            path = self.base_path / connector / dataset_name
        path.mkdir(parents=True, exist_ok=True)
        
        file_path = path / f"{run_id}.parquet"
        pq.write_table(data, file_path)
        
        logger.info(f"Saved {len(data)} rows to {file_path}")
        return str(file_path)
    
    def save_snapshot(self, snapshot: dict, dataset_name: str, partition: str = None) -> str:
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        snapshot_path = self.base_path / connector / "snapshots"
        if partition:
            snapshot_path = snapshot_path / dataset_name / partition
        snapshot_path.mkdir(parents=True, exist_ok=True)
        
        snapshot_file = snapshot_path / f"{run_id}.json"
        with open(snapshot_file, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        logger.info(f"Wrote snapshot to {snapshot_file}")
        return str(snapshot_file)
    
    def load_asset(self, connector: str, asset_name: str, run_id: str = None) -> pa.Table:
        base_path = self.base_path / connector / asset_name
        if not base_path.exists():
            raise FileNotFoundError(f"No asset data found at {base_path}")
        
        # Find available parquet files
        parquet_files = list(base_path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {base_path}")
        
        if run_id:
            # Load specific run
            file_path = base_path / f"{run_id}.parquet"
            if not file_path.exists():
                raise FileNotFoundError(f"Run {run_id} not found for asset {asset_name}")
        else:
            # Load most recent file
            file_path = max(parquet_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Loading most recent asset from {file_path}")
        
        return pq.read_table(file_path)


class R2Storage:
    """Cloudflare R2 storage for production"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            endpoint_url=os.environ["R2_ENDPOINT_URL"]
        )
        self.bucket = os.environ["R2_BUCKET_NAME"]
        logger.info(f"R2Storage initialized for bucket {self.bucket}")
    
    def upload_data(self, data: pa.Table, dataset_name: str, partition: str = None) -> str:
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        buffer = pa.BufferOutputStream()
        pq.write_table(data, buffer)
        
        # Build key with optional partition
        if partition:
            key = f"{connector}/{dataset_name}/{partition}/{run_id}.parquet"
        else:
            key = f"{connector}/{dataset_name}/{run_id}.parquet"
        
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue().to_pybytes()
        )
        
        return key
    
    def save_snapshot(self, snapshot: dict, dataset_name: str, partition: str = None) -> str:
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        if partition:
            snapshot_key = f"{connector}/.snapshots/{dataset_name}/{partition}/{run_id}.json"
        else:
            snapshot_key = f"{connector}/.snapshots/{dataset_name}_{run_id}.json"
        
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=snapshot_key,
            Body=json.dumps(snapshot, indent=2).encode(),
            ContentType="application/json"
        )
        
        logger.info(f"Wrote snapshot to {snapshot_key}")
        return snapshot_key
    
    def load_asset(self, connector: str, asset_name: str, run_id: str = None) -> pa.Table:
        if run_id:
            # Load specific run
            key = f"{connector}/{asset_name}/{run_id}.parquet"
        else:
            # List objects to find most recent
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"{connector}/{asset_name}/",
                Delimiter="/"
            )
            
            if 'Contents' not in response or not response['Contents']:
                raise FileNotFoundError(f"No asset data found for {connector}/{asset_name}")
            
            # Get most recent file by LastModified
            objects = response['Contents']
            most_recent = max(objects, key=lambda obj: obj['LastModified'])
            key = most_recent['Key']
            logger.info(f"Loading most recent asset from {key}")
        
        # Download and read the parquet file
        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=key
        )
        return pq.read_table(pa.BufferReader(response['Body'].read()))


def _get_storage():
    """Get or create the storage backend based on STORAGE_BACKEND env var"""
    global _storage_backend
    
    if _storage_backend is None:
        storage_backend = os.environ['STORAGE_BACKEND']
        
        if storage_backend == 'local':
            _storage_backend = LocalStorage()
        elif storage_backend == 'r2':
            _storage_backend = R2Storage()
        else:
            raise ValueError(f"Unknown STORAGE_BACKEND: {storage_backend}")
    
    return _storage_backend


def _generate_snapshot(data: pa.Table, dataset_name: str) -> dict:
    """Generate detailed snapshot/profile of the dataset"""
    snapshot = {
        "dataset": dataset_name,
        "timestamp": datetime.now().isoformat(),
        "row_count": len(data),
        "column_count": len(data.schema),
        "memory_usage_mb": round(data.nbytes / 1024 / 1024, 2),
        "columns": []
    }
    
    for field in data.schema:
        col = data[field.name]
        col_info = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
            "null_count": pc.count(pc.is_null(col)).as_py()
        }
        
        # Add cardinality for non-numeric types
        if not (pa.types.is_integer(field.type) or pa.types.is_floating(field.type)) and not pa.types.is_temporal(field.type):
            col_info["cardinality"] = pc.count_distinct(col).as_py()
            # Sample values for string types
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                unique_vals = pc.unique(col)
                col_info["sample_values"] = unique_vals.slice(0, min(5, len(unique_vals))).to_pylist()
        
        # Add statistics for numeric types
        elif pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            non_null = pc.drop_null(col)
            if len(non_null) > 0:
                col_info["min"] = pc.min(non_null).as_py()
                col_info["max"] = pc.max(non_null).as_py()
                col_info["mean"] = round(pc.mean(non_null).as_py(), 4)
                col_info["stddev"] = round(pc.stddev(non_null).as_py(), 4) if len(non_null) > 1 else 0
        
        # Add range for temporal types
        elif pa.types.is_temporal(field.type):
            non_null = pc.drop_null(col)
            if len(non_null) > 0:
                col_info["min"] = str(pc.min(non_null).as_py())
                col_info["max"] = str(pc.max(non_null).as_py())
        
        snapshot["columns"].append(col_info)
    
    # Add sample rows
    sample_size = min(10, len(data))
    if sample_size > 0:
        snapshot["sample_rows"] = data.slice(0, sample_size).to_pylist()
    
    return snapshot


# Public API functions - thin wrappers around storage backend
def upload_data(data: pa.Table, dataset_name: str, partition: str = None) -> str:
    """Upload data to configured storage backend
    
    Args:
        data: The data to upload as a PyArrow table
        dataset_name: Logical dataset name (e.g., "page_views")
        partition: Optional partition path (e.g., "2024/01/15")
    
    Returns:
        str: The storage path where data was saved
    """
    # Upload data
    storage = _get_storage()
    key = storage.upload_data(data, dataset_name, partition)
    
    # Log data output
    schema_info = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in data.schema
    ]
    
    metrics = {}
    if partition:
        metrics['partition'] = partition
    
    debug.log_data_output(
        dataset_name=dataset_name,
        row_count=len(data),
        column_count=len(data.schema),
        size_bytes=data.nbytes,
        storage_path=key,
        schema=schema_info,
        metrics=metrics
    )
    
    # Generate and save snapshot if enabled
    if os.environ.get('WRITE_SNAPSHOT', '').lower() == 'true':
        snapshot = _generate_snapshot(data, dataset_name)
        if partition:
            snapshot['partition'] = partition
        storage.save_snapshot(snapshot, dataset_name, partition)
    
    return key


def load_state(asset: str) -> dict:
    """Load state for an asset from local filesystem.
    
    State is always stored locally to enable version control via Git commits
    in GitHub Actions, providing transparency and audit trail.
    """
    environment = os.environ.get('STORAGE_BACKEND', 'local')
    state_file = Path(".state") / environment / f"{asset}.json"
    
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
    environment = os.environ.get('STORAGE_BACKEND', 'local')
    state_dir = Path(".state") / environment
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


def publish_to_subsets(data: pa.Table, dataset_name: str, schema_path: str = None) -> None:
    """Publish data to Subsets for external consumption.
    
    This is different from upload_data() which stores data for internal processing.
    Publishing makes data available for end users via the Subsets platform.
    
    Args:
        data: The data to upload to Subsets
        dataset_name: Name of the dataset in Subsets
        schema_path: Optional path to schema JSON. If None, looks in schemas/{dataset_name}.json
        
    Raises:
        ImportError: If subsets_client is not installed
        FileNotFoundError: If schema file is not found
        ValueError: If required environment variables are missing
    """
    if SubsetsClient is None:
        logger.warning("subsets_client not installed. Skipping Subsets publish.")
        return
    
    # Check required environment variables
    api_key = os.environ.get("SUBSETS_API_KEY")
    if not api_key:
        logger.warning("SUBSETS_API_KEY not set. Skipping Subsets publish.")
        return
    
    # Determine schema path
    if schema_path is None:
        schema_path = f"schemas/{dataset_name}.json"
    
    schema_path = Path(schema_path)
    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema required for Subsets publish.\n"
            f"Please create: {schema_path}\n"
            f"Example format:\n"
            f'{{\n'
            f'  "title": "Dataset Title",\n'
            f'  "description": "Dataset description",\n'
            f'  "columns": [\n'
            f'    {{\n'
            f'      "id": "column_name",\n'
            f'      "type": "string",\n'
            f'      "description": "Column description",\n'
            f'      "nullable": true\n'
            f'    }}\n'
            f'  ]\n'
            f'}}'
        )
    
    # Load schema
    with open(schema_path, 'r') as f:
        schema_def = json.load(f)
    
    # Initialize Subsets client
    client = SubsetsClient(
        api_key=api_key,
        base_url=os.environ.get("SUBSETS_API_URL", "https://api.subsets.com")
    )
    
    # Create full dataset ID
    connector = os.environ.get('CONNECTOR_NAME', 'unknown')
    dataset_id = f"{connector}.{dataset_name}"
    
    # Check if dataset exists, create if not
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists in Subsets")
    except Exception:
        # Dataset doesn't exist, create it
        logger.info(f"Creating new dataset in Subsets: {dataset_id}")
        
        # Convert schema to Subsets format
        columns = []
        for col in schema_def.get("columns", []):
            columns.append({
                "name": col.get("id", col.get("name")),  # Support both 'id' and 'name'
                "type": _json_to_subsets_type(col.get("type", "string")),
                "nullable": col.get("nullable", True),
                "description": col.get("description", "")
            })
        
        client.create_dataset(
            dataset_id=dataset_id,
            name=schema_def.get("title", f"{connector} - {dataset_name}"),
            description=schema_def.get("description", f"Data from {connector} connector"),
            columns=columns
        )
    
    # Publish data
    if len(data) > 0:
        logger.info(f"Publishing {len(data)} rows to Subsets dataset {dataset_id}")
        client.append_data(dataset_id, data)
        logger.info(f"Successfully published data to Subsets")
    else:
        logger.info(f"No data to publish to Subsets (empty dataset)")


def _json_to_subsets_type(json_type: str) -> str:
    """Convert JSON schema type to Subsets type"""
    type_mapping = {
        "string": "string",
        "integer": "integer",
        "float": "double",
        "double": "double",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "datetime": "timestamp"
    }
    return type_mapping.get(json_type.lower(), "string")