import os
import json
import boto3
import uuid
import gzip
from io import BytesIO, StringIO
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Set, Protocol
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from urllib.parse import unquote

logger = Logger()
tracer = Tracer()


# Exceptions
class StorageError(Exception): pass


class ValidationError(Exception): pass


class PartitionError(Exception): pass


# Config
@dataclass
class StorageConfig:
    chunk_size: int = 1000000
    reserved_words: Set[str] = None
    default_storage_method: str = 'concat'

    def __post_init__(self):
        if self.reserved_words is None:
            self.reserved_words = {'log', 'default'}


# Storage Interface
class StorageBackend(Protocol):
    def store(self, path: str, content: bytes) -> str: pass

    def get(self, path: str) -> bytes: pass

    def delete(self, path: str) -> None: pass

    def list(self, prefix: str) -> List[str]: pass


class S3Storage(StorageBackend):
    def __init__(self, bucket: str):
        self.s3 = boto3.client('s3')
        self.bucket = bucket

    def store(self, path: str, content: bytes) -> str:
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=content)
        return path

    def get(self, path: str) -> bytes:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=path)['Body'].read()
        except Exception as e:
            raise StorageError(f"Failed to get {path}: {str(e)}")

    def delete(self, path: str) -> None:
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=path)
        except Exception as e:
            raise StorageError(f"Failed to delete {path}: {str(e)}")

    def list(self, prefix: str) -> List[str]:
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            objects = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if 'Contents' in page:
                    objects.extend([obj['Key'] for obj in page['Contents']])
            return objects
        except Exception as e:
            raise StorageError(f"Failed to list {prefix}: {str(e)}")


# Partition Strategies
class PartitionStrategy(ABC):
    @abstractmethod
    def partition(self, df: pd.DataFrame, column: str, storage: StorageBackend, base_path: str) -> List[
        Dict[str, Any]]: pass


class DatePartitionStrategy(PartitionStrategy):
    def __init__(self, config: StorageConfig):
        self.config = config

    def partition(self, df: pd.DataFrame, column: str, storage: StorageBackend, base_path: str) -> List[Dict[str, Any]]:
        if column not in df.columns:
            raise ValidationError(f"Column '{column}' not found")

        try:
            df[column] = pd.to_datetime(df[column])
        except Exception as e:
            raise ValidationError(f"Invalid date format in column '{column}': {str(e)}")

        chunks_info = []
        for date, date_df in df.groupby(pd.Grouper(key=column, freq='D')):
            if len(date_df) == 0:
                continue

            date_str = date.strftime('%Y-%m-%d')
            path = f"{base_path}/data/{column}/{date_str}"

            for i in range(0, len(date_df), self.config.chunk_size):
                chunk_df = date_df.iloc[i:i + self.config.chunk_size]
                chunk_uuid = str(uuid.uuid4())
                chunk_path = f"{path}/{chunk_uuid}.csv.gz"

                buffer = BytesIO()
                with gzip.GzipFile(fileobj=buffer, mode='w') as gz:
                    chunk_df.to_csv(gz, index=False)

                storage.store(chunk_path, buffer.getvalue())
                chunks_info.append({
                    'path': chunk_path,
                    'rows': len(chunk_df),
                    'date': date_str
                })

        return chunks_info


class IDPartitionStrategy(PartitionStrategy):
    def __init__(self, config: StorageConfig):
        self.config = config

    def partition(self, df: pd.DataFrame, column: str, storage: StorageBackend, base_path: str) -> List[Dict[str, Any]]:
        if column not in df.columns:
            raise ValidationError(f"Column '{column}' not found")

        if not pd.api.types.is_numeric_dtype(df[column]):
            raise ValidationError(f"Column '{column}' must be numeric")

        df = df.sort_values(column)
        chunks_info = []

        for i in range(0, len(df), self.config.chunk_size):
            chunk_df = df.iloc[i:i + self.config.chunk_size]
            min_id = chunk_df[column].iloc[0]
            max_id = chunk_df[column].iloc[-1]

            path = f"{base_path}/data/{column}/from_{min_id}_to_{max_id}"
            chunk_uuid = str(uuid.uuid4())
            chunk_path = f"{path}/{chunk_uuid}.csv.gz"

            buffer = BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode='w') as gz:
                chunk_df.to_csv(gz, index=False)

            storage.store(chunk_path, buffer.getvalue())
            chunks_info.append({
                'path': chunk_path,
                'rows': len(chunk_df),
                'min_id': min_id,
                'max_id': max_id
            })

        return chunks_info


# Filters
@dataclass
class DateFilter:
    start_date: datetime
    end_date: datetime

    @classmethod
    def from_str(cls, start: str, end: str) -> 'DateFilter':
        try:
            return cls(
                start_date=pd.to_datetime(start),
                end_date=pd.to_datetime(end)
            )
        except ValueError as e:
            raise ValidationError(f"Invalid date format: {str(e)}")


@dataclass
class LogFilter:
    start_time: str
    end_time: str


# Main Storage Class
class DataFrameStorage:
    def __init__(self, bucket_name: str, config: Optional[StorageConfig] = None):
        self.storage = S3Storage(bucket_name)
        self.config = config or StorageConfig()
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ['METADATA_TABLE'])

        self.partition_strategies = {
            'Date': DatePartitionStrategy(self.config),
            'ID': IDPartitionStrategy(self.config)
        }

    def _path_exists_in_dynamo(self, user_id: str, df_name: str) -> bool:
        """Check if the df_name exists in DynamoDB."""
        try:
            response = self.table.get_item(Key={'user_id': user_id, 'df_path': df_name})
            return 'Item' in response
        except Exception as e:
            logger.error(f"Error checking DataFrame existence: {str(e)}")
            return False

    def store_dataframe(self, user_id: str, df: pd.DataFrame, df_name: str,
                       columns_keys: Dict[str, str] = None) -> Dict[str, Any]:
        # Convert numeric columns to Python native types
        for col in df.select_dtypes(include=['integer', 'floating']).columns:
            df[col] = df[col].map(convert_numpy_types)

        metadata = self._init_metadata(df, df_name, columns_keys)

        # Store by partition type
        for col, part_type in (columns_keys or {}).items():
            if part_type not in self.partition_strategies:
                raise ValidationError(f"Unknown partition type: {part_type}")

            strategy = self.partition_strategies[part_type]
            chunks = strategy.partition(df, col, self.storage, df_name)

            metadata['partitions'][f"{part_type.lower()}_{col}"] = {
                'type': part_type.lower(),
                'column': col,
                'chunks': chunks
            }

        metadata = json.loads(
            json.dumps(metadata, default=convert_numpy_types)
        )
        self._store_metadata(user_id, df_name, metadata)
        return metadata

    def get_dataframe(
            self,
            user_id: str,
            df_name: str,
            partition_info: Dict[str, Any] = None
    ) -> pd.DataFrame:
        """
        Retrieves dataframe from S3 using only S3 operations, using parallel processing
        for better performance.
        """
        # Initial check - return empty DataFrame if not in DynamoDB
        if not self._path_exists_in_dynamo(user_id, df_name):
            logger.warning(f"DataFrame {df_name} not found for user {user_id}")
            return pd.DataFrame()

        if not partition_info or 'partition_type' not in partition_info or 'column' not in partition_info:
            return pd.DataFrame()

        base_path = f"{df_name}/data"
        partition_type = partition_info['partition_type'].lower()
        column = partition_info['column']
        prefix = f"{base_path}/{column}"

        # Get all keys and filter relevant ones
        keys = self._list_relevant_keys(prefix, partition_info)
        if not keys:
            return pd.DataFrame()

        # Use ThreadPoolExecutor to read files in parallel
        dfs = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for key in keys:
                futures.append(
                    executor.submit(self._read_csv_from_s3, key)
                )

            for future in futures:
                df = future.result()
                if df is not None and len(df) > 0:
                    filtered_df = self._apply_filters(df, partition_info)
                    if len(filtered_df) > 0:
                        dfs.append(filtered_df)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def _list_relevant_keys(self, prefix: str, partition_info: Dict[str, Any]) -> List[str]:
        """List all relevant S3 keys based on partition type and filters."""
        all_keys = self.storage.list(prefix)
        partition_type = partition_info['partition_type'].lower()

        if not partition_info.get('start_date') and not partition_info.get('end_date') and not partition_info.get(
                'values'):
            return all_keys

        relevant_keys = []
        for key in all_keys:
            if partition_type == 'date':
                try:
                    key_date = key.split('/')[-2]  # Assumes date is second-to-last part
                    is_relevant = True

                    if partition_info.get('start_date'):
                        is_relevant = is_relevant and key_date >= partition_info['start_date']
                    if partition_info.get('end_date'):
                        is_relevant = is_relevant and key_date <= partition_info['end_date']

                    if is_relevant:
                        relevant_keys.append(key)
                except:
                    continue

            elif partition_type == 'id':
                if partition_info.get('values'):
                    try:
                        range_part = key.split('/')[-2]
                        min_id = float(range_part.split('_')[1])
                        max_id = float(range_part.split('_')[-1])
                        values = json.loads(partition_info['values'])
                        if any(min_id <= float(v) <= max_id for v in values):
                            relevant_keys.append(key)
                    except:
                        continue
                else:
                    relevant_keys.append(key)

        return relevant_keys

    def _read_csv_from_s3(self, key: str) -> Optional[pd.DataFrame]:
        """Read a CSV file from S3."""
        try:
            content = self.storage.get(key)
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return pd.read_csv(StringIO(gz.read().decode('utf-8')))
        except Exception as e:
            logger.error(f"Error reading key {key}: {str(e)}")
            return None

    def _apply_filters(self, df: pd.DataFrame, partition_info: Dict[str, Any]) -> pd.DataFrame:
        """Apply filters to the dataframe based on partition info."""
        if len(df) == 0:
            return df

        partition_type = partition_info['partition_type'].lower()
        column = partition_info['column']

        if partition_type == 'date':
            df[column] = pd.to_datetime(df[column])

            # Client will send both dates - we need to handle them even if they're the same
            if partition_info.get('start_date'):
                mask = (df[column] >= partition_info['start_date'])
                df = df[mask]

            if partition_info.get('end_date'):
                mask = (df[column] <= partition_info['end_date'])
                df = df[mask]

        elif partition_type == 'id':
            if partition_info.get('values'):
                df[column] = df[column].astype(float)
                values = [float(v) for v in json.loads(partition_info['values'])]
                return df[df[column].isin(values)]

        return df

    def _init_metadata(self, df: pd.DataFrame, df_name: str, columns_keys: Dict[str, str]) -> Dict[str, Any]:
        return {
            'df_name': df_name,
            'columns_keys': columns_keys or {},
            'total_rows': len(df),
            'partitions': {}
        }

    def _store_metadata(self, user_id: str, df_name: str, metadata: Dict[str, Any]) -> None:
        self.table.put_item(Item={
            'user_id': user_id,
            'df_path': df_name,
            'metadata': metadata,
            'created_at': datetime.utcnow().isoformat()
        })

    def _get_chunks(self, user_id: str, df_name: str, partition_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        metadata = self._get_metadata(user_id, df_name)
        if not partition_info:
            return []

        partition_key = f"{partition_info['type']}_{partition_info['column']}"
        if partition_key not in metadata['partitions']:
            raise StorageError(f"Partition not found: {partition_key}")

        return metadata['partitions'][partition_key]['chunks']

    def _get_metadata(self, user_id: str, df_name: str) -> Dict[str, Any]:
        response = self.table.get_item(Key={'user_id': user_id, 'df_path': df_name})
        if 'Item' not in response:
            raise StorageError(f"DataFrame not found: {df_name}")
        return response['Item']['metadata']


# Lambda Handlers
def validate_request(event: APIGatewayProxyEvent) -> str:
    if 'authorizer' not in event.get('requestContext', {}) or \
            'claims' not in event['requestContext']['authorizer']:
        raise ValidationError("Unauthorized")
    return event['requestContext']['authorizer']['claims']['sub']


def convert_numpy_types(obj):
    """Convert numpy types to native Python types"""
    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                        np.int16, np.int32, np.int64, np.uint8,
                        np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def upload(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    try:
        user_id = validate_request(event)
        storage = DataFrameStorage(f"df-{user_id}")

        if not event.get('body'):
            raise ValidationError("Missing request body")

        body = json.loads(event['body'])
        if 'dataframe' not in body or 'dataframe_name' not in body:
            raise ValidationError("Missing required fields")

        # Parse the JSON data back to DataFrame
        df = pd.read_json(body['dataframe'])

        # Convert numeric columns to Python native types
        for col in df.select_dtypes(include=['integer', 'floating']).columns:
            df[col] = df[col].map(convert_numpy_types)

        metadata = storage.store_dataframe(
            user_id=user_id,
            df=df,
            df_name=body['dataframe_name'],
            columns_keys=body.get('columns_keys')
        )

        # Convert numpy types in metadata before JSON serialization
        metadata = json.loads(
            json.dumps(metadata, default=convert_numpy_types)
        )

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(metadata)
        }

    except Exception as e:
        logger.exception("Error in upload")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def get(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    try:
        user_id = validate_request(event)
        storage = DataFrameStorage(f"df-{user_id}")

        df_name = unquote(event['pathParameters']['name'])
        params = event.get('queryStringParameters', {}) or {}

        # Construct partition info from query parameters
        partition_info = {}
        if params.get('partition_type'):
            partition_info = {
                'partition_type': params.get('partition_type'),
                'column': params.get('column'),
                'start_date': params.get('start_date'),
                'end_date': params.get('end_date'),
                'partition_value': params.get('partition_value')
            }
            # Remove None values
            partition_info = {k: v for k, v in partition_info.items() if v is not None}

        df = storage.get_dataframe(
            user_id=user_id,
            df_name=df_name,
            partition_info=partition_info
        )

        if len(df) > 0:
            # The client expects just a DataFrame to convert directly
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': df.to_json(orient='records')
            }
        else:
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': '[]'
            }

    except Exception as e:
        logger.exception("Error in get")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
