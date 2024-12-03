import os
import json
import boto3
import pandas as pd
import numpy as np
from datetime import datetime
import uuid
import gzip
from io import BytesIO
from typing import Dict, Any, List, Optional
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from urllib.parse import unquote

logger = Logger()
tracer = Tracer()

class DataFrameStorageError(Exception):
    pass

class DataFrameStorage:
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ['METADATA_TABLE'])
        self.bucket = bucket_name
        self.chunk_size = int(os.environ.get('CHUNK_SIZE', 1000000))
        self.reserved_words = {'log', 'default'}

    def _validate_column_names(self, columns_keys: Dict[str, str]) -> None:
        reserved = set(columns_keys.keys()) & self.reserved_words
        if reserved:
            raise ValueError(f"Column names cannot use reserved words: {reserved}")

    def store_chunk(self, df: pd.DataFrame, path: str) -> str:
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='w') as gz:
            df.to_csv(gz, index=False)
        chunk_path = f"{path}/{str(uuid.uuid4())}.csv.gz"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=chunk_path,
            Body=buffer.getvalue(),
            ContentType='application/gzip'
        )
        return chunk_path

    def _delete_path_contents(self, path: str) -> None:
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket, Prefix=path):
                objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
                if objects:
                    self.s3.delete_objects(
                        Bucket=self.bucket,
                        Delete={'Objects': objects}
                    )
        except Exception as e:
            logger.warning(f"Error deleting contents at {path}: {str(e)}")

    def _store_log(self, df: pd.DataFrame, df_name: str) -> Dict[str, Any]:
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')
        log_path = f"{df_name}/logs/{timestamp}"
        
        chunks_info = []
        for i in range(0, len(df), self.chunk_size):
            chunk_df = df.iloc[i:i+self.chunk_size]
            chunk_path = self.store_chunk(chunk_df, log_path)
            chunks_info.append({
                'path': chunk_path,
                'rows': len(chunk_df),
                'timestamp': timestamp
            })
        
        return {
            'timestamp': timestamp,
            'chunks': chunks_info
        }

    def _store_date_partitioned(self, df: pd.DataFrame, base_path: str, date_column: str, storage_method: str) -> List[
        Dict[str, Any]]:
        chunks_info = []

        # Validate date_column exists
        if date_column not in df.columns:
            raise ValueError(f"Column '{date_column}' not found in DataFrame")

        # Convert to datetime with validation
        try:
            df[date_column] = pd.to_datetime(df[date_column])
        except Exception as e:
            raise ValueError(f"Failed to convert '{date_column}' to datetime: {str(e)}")

        # Group and process data
        for date, date_df in df.groupby(pd.Grouper(key=date_column, freq='D')):
            if len(date_df) == 0:
                continue

            # Format date string with validation
            try:
                date_str = date.strftime('%Y-%m-%d')
            except Exception as e:
                raise ValueError(f"Failed to format date '{date}' to string: {str(e)}")

            path = f"{base_path}/data/{date_column}/{date_str}"

            # Handle storage method
            if storage_method == 'keep_last':
                try:
                    self._delete_path_contents(path)
                except Exception as e:
                    raise DataFrameStorageError(f"Failed to delete existing data at '{path}': {str(e)}")

            # Store chunks
            for i in range(0, len(date_df), self.chunk_size):
                try:
                    chunk_df = date_df.iloc[i:i + self.chunk_size]
                    chunk_path = self.store_chunk(chunk_df, path)
                    chunks_info.append({
                        'path': chunk_path,
                        'rows': len(chunk_df),
                        'date': date_str
                    })
                except Exception as e:
                    raise DataFrameStorageError(f"Failed to store chunk at '{path}': {str(e)}")

        if not chunks_info:
            raise ValueError(f"No valid data found for date column '{date_column}'")

        return chunks_info

    def _store_id_partitioned(self, df: pd.DataFrame, base_path: str, id_column: str, storage_method: str) -> List[
        Dict[str, Any]]:
        # Validate id_column exists
        if id_column not in df.columns:
            raise ValueError(f"Column '{id_column}' not found in DataFrame")

        # Validate ID column data
        if not pd.api.types.is_numeric_dtype(df[id_column]):
            raise ValueError(f"Column '{id_column}' must contain numeric IDs")

        try:
            df = df.sort_values(id_column)
        except Exception as e:
            raise ValueError(f"Failed to sort DataFrame by '{id_column}': {str(e)}")

        chunks_info = []
        current_chunk_start = 0

        while current_chunk_start < len(df):
            chunk_end = min(current_chunk_start + self.chunk_size, len(df))
            chunk_df = df.iloc[current_chunk_start:chunk_end]

            try:
                min_id = chunk_df[id_column].iloc[0]
                max_id = chunk_df[id_column].iloc[-1]
            except Exception as e:
                raise ValueError(f"Failed to get min/max IDs from chunk: {str(e)}")

            path = f"{base_path}/data/{id_column}/from_{min_id}_to_{max_id}"

            if storage_method == 'keep_last':
                try:
                    self._delete_path_contents(path)
                except Exception as e:
                    raise DataFrameStorageError(f"Failed to delete existing data at '{path}': {str(e)}")

            try:
                chunk_path = self.store_chunk(chunk_df, path)
                chunks_info.append({
                    'path': chunk_path,
                    'rows': len(chunk_df),
                    'min_id': min_id,
                    'max_id': max_id
                })
            except Exception as e:
                raise DataFrameStorageError(f"Failed to store chunk at '{path}': {str(e)}")

            current_chunk_start = chunk_end

        if not chunks_info:
            raise ValueError(f"No data stored for ID column '{id_column}'")

        return chunks_info

    def _store_version_controlled(self, df: pd.DataFrame, base_path: str, keep_last: bool) -> Dict[str, Any]:
        now = datetime.utcnow()
        version_path = now.strftime('%Y-%m-%d/%H:%M:%S')
        full_path = f"{base_path}/data/default/{version_path}"
        
        if keep_last:
            try:
                last_key_path = f"{base_path}/data/default/last_key.txt"
                response = self.s3.get_object(Bucket=self.bucket, Key=last_key_path)
                last_version = response['Body'].read().decode('utf-8')
                self._delete_path_contents(f"{base_path}/data/default/{last_version}")
            except self.s3.exceptions.NoSuchKey:
                pass
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=last_key_path,
                Body=version_path.encode('utf-8')
            )
        
        chunks_info = []
        for i in range(0, len(df), self.chunk_size):
            chunk_df = df.iloc[i:i+self.chunk_size]
            chunk_path = self.store_chunk(chunk_df, full_path)
            chunks_info.append({
                'path': chunk_path,
                'rows': len(chunk_df),
                'version': version_path
            })
            
        return {
            'version_path': version_path,
            'chunks': chunks_info
        }

    def store_dataframe(
        self,
        user_id: str,
        df: pd.DataFrame,
        df_name: str,
        columns_keys: Dict[str, str] = None,
        external_key: str = 'NOW',
        keep_last: bool = False,
        storage_method: str = 'concat'
    ) -> Dict[str, Any]:
        if storage_method not in ['concat', 'keep_last']:
            raise ValueError("storage_method must be either 'concat' or 'keep_last'")

        if columns_keys:
            self._validate_column_names(columns_keys)

        metadata = {
            'df_name': df_name,
            'columns_keys': columns_keys or {},
            'external_key': external_key,
            'keep_last': keep_last,
            'storage_method': storage_method,
            'total_rows': len(df),
            'partitions': {}
        }

        # Store log copy first
        log_info = self._store_log(df, df_name)
        metadata['log'] = log_info

        # Handle default version-controlled storage
        if not columns_keys or external_key == 'NOW':
            base_path = f"{df_name}/external_key"
            version_info = self._store_version_controlled(df, base_path, keep_last)
            metadata['partitions']['version'] = version_info
        
        # Handle date partitioning
        date_columns = {k: v for k, v in (columns_keys or {}).items() if v == 'Date'}
        for date_col in date_columns:
            base_path = f"{df_name}"
            chunks_info = self._store_date_partitioned(df, base_path, date_col, storage_method)
            metadata['partitions'][f'date_{date_col}'] = {
                'type': 'date',
                'column': date_col,
                'chunks': chunks_info
            }

        # Handle ID partitioning
        id_columns = {k: v for k, v in (columns_keys or {}).items() if v == 'ID'}
        for id_col in id_columns:
            base_path = f"{df_name}"
            chunks_info = self._store_id_partitioned(df, base_path, id_col, storage_method)
            metadata['partitions'][f'id_{id_col}'] = {
                'type': 'id',
                'column': id_col,
                'chunks': chunks_info
            }

        # Store metadata
        self.table.put_item(Item={
            'user_id': user_id,
            'df_path': df_name,
            'metadata': metadata,
            'created_at': datetime.utcnow().isoformat()
        })
        
        return metadata

    def get_metadata(
        self,
        user_id: str,
        df_name: str,
        partition_type: Optional[str] = None,
        external_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        response = self.table.get_item(
            Key={
                'user_id': user_id,
                'df_path': df_name
            }
        )
        
        if 'Item' not in response:
            raise DataFrameStorageError(f"DataFrame {df_name} not found")
            
        metadata = response['Item']['metadata']
        
        if external_key == 'NOW':
            base_path = f"{df_name}/external_key/data/default"
            try:
                response = self.s3.get_object(
                    Bucket=self.bucket,
                    Key=f"{base_path}/last_key.txt"
                )
                version_path = response['Body'].read().decode('utf-8')
                metadata['current_version'] = version_path
            except self.s3.exceptions.NoSuchKey:
                pass
        
        if partition_type and external_key:
            partition_key = f"{partition_type.lower()}_{external_key}"
            if partition_key in metadata['partitions']:
                return {
                    'df_name': df_name,
                    'partition_type': partition_type,
                    'column': external_key,
                    'chunks': metadata['partitions'][partition_key]['chunks']
                }
        
        return metadata

    def get_chunk(self, chunk_path: str) -> bytes:
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=chunk_path
            )
            return response['Body'].read()
        except Exception as e:
            logger.error(f"Error retrieving chunk: {str(e)}")
            raise DataFrameStorageError(f"Error retrieving chunk: {str(e)}")

def validate_request(event: APIGatewayProxyEvent) -> str:
    if 'authorizer' not in event.get('requestContext', {}) or \
       'claims' not in event['requestContext']['authorizer']:
        raise ValueError("Unauthorized")
    return event['requestContext']['authorizer']['claims']['sub']

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def upload(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
   try:
       user_id = validate_request(event)
       bucket_name = f"df-{user_id}"
       storage = DataFrameStorage(bucket_name)
       
       if not event.get('body'):
           raise ValueError("Missing request body")
           
       body = json.loads(event['body'])
       
       if 'dataframe' not in body or 'dataframe_name' not in body:
           raise ValueError("Missing required fields")
           
       df = pd.read_json(body['dataframe'])
       df_name = body['dataframe_name']
       columns_keys = body.get('columns_keys', {})
       external_key = body.get('external_key', 'NOW')
       keep_last = body.get('keep_last', False)
       
       metadata = storage.store_dataframe(
           user_id,
           df,
           df_name,
           columns_keys,
           external_key,
           keep_last
       )
       
       return {
           'statusCode': 200,
           'headers': {
               'Content-Type': 'application/json',
               'Access-Control-Allow-Origin': '*'
           },
           'body': json.dumps(metadata)
       }
       
   except Exception as e:
       logger.exception("Error in upload handler")
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
       bucket_name = f"df-{user_id}"
       storage = DataFrameStorage(bucket_name)
       
       df_name = unquote(event['pathParameters']['name'])
       query_params = event.get('queryStringParameters', {}) or {}
       
       if query_params.get('metadata') == 'true':
           metadata = storage.get_metadata(
               user_id,
               df_name,
               partition_type=query_params.get('partition_type'),
               external_key=query_params.get('external_key')
           )
           
           return {
               'statusCode': 200,
               'headers': {
                   'Content-Type': 'application/json',
                   'Access-Control-Allow-Origin': '*'
               },
               'body': json.dumps(metadata)
           }
           
       elif chunk_path := query_params.get('chunk_path'):
           chunk_data = storage.get_chunk(chunk_path)
           
           return {
               'statusCode': 200,
               'headers': {
                   'Content-Type': 'application/gzip',
                   'Access-Control-Allow-Origin': '*'
               },
               'body': chunk_data,
               'isBase64Encoded': True
           }
           
       else:
           return {
               'statusCode': 400,
               'headers': {'Access-Control-Allow-Origin': '*'},
               'body': json.dumps({
                   'error': 'Must specify either metadata=true or chunk_path parameter'
               })
           }
       
   except Exception as e:
       logger.exception("Error in get handler")
       return {
           'statusCode': 500,
           'headers': {'Access-Control-Allow-Origin': '*'},
           'body': json.dumps({'error': str(e)})
       }
