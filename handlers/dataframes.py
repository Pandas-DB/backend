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
from pathlib import Path
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from urllib.parse import unquote
 	
logger = Logger()
tracer = Tracer()

class DataFrameStorage:
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(os.environ['METADATA_TABLE'])
        self.bucket = bucket_name
        self.chunk_size = int(os.environ['CHUNK_SIZE'])
        
    def ensure_bucket_exists(self):
        """Ensure user's bucket exists"""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except:
            self.s3.create_bucket(
                Bucket=self.bucket,
                CreateBucketConfiguration={
                    'LocationConstraint': os.environ['DEPLOYMENT_REGION']
                }
            )

    def store_chunk(self, df: pd.DataFrame, path: str) -> None:
        """Store DataFrame chunk as compressed CSV"""
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='w') as gz:
            df.to_csv(gz, index=False)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=path,
            Body=buffer.getvalue(),
            ContentType='application/gzip'
        )

    def _generate_chunk_path(self, base_path: str, chunk_id: str) -> str:
        """Generate path for chunk storage"""
        return f"{base_path}/{chunk_id}.csv.gz"

    def _store_metadata(
        self,
        user_id: str,
        df_name: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Store DataFrame metadata in DynamoDB"""
        self.table.put_item(Item={
            'user_id': user_id,
            'df_path': df_name,
            'metadata': metadata,
            'created_at': datetime.utcnow().isoformat()
        })

    def store_dataframe(
            self,
            user_id: str,
            df: pd.DataFrame,
            df_name: str,
            columns_keys: Dict[str, str],
            external_key: str = 'NOW',
            keep_last: bool = False
    ) -> Dict[str, Any]:
        """Store DataFrame with versioning support"""
        try:
            logger.debug(f"Starting store_dataframe for user {user_id}")
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"DataFrame name: {df_name}")
            logger.debug(f"Columns keys: {columns_keys}")

            self.ensure_bucket_exists()
            logger.debug(f"Bucket {self.bucket} exists")

            stored_paths = []
            metadata = {
                'df_name': df_name,
                'columns_keys': columns_keys,
                'external_key': external_key,
                'keep_last': keep_last,
                'total_rows': len(df),
                'chunks': []
            }
            logger.debug(f"Created metadata: {metadata}")

            # Generate base paths
            if external_key == 'NOW':
                now = datetime.utcnow()
                date_str = now.strftime('%Y-%m-%d')
                time_str = now.strftime('%H:%M:%S')
                base_path = f"{df_name}/external_key/default/{date_str}"
                version_path = f"{time_str}"
            else:
                base_path = f"{df_name}/external_key/{external_key}"
                version_path = str(uuid.uuid4())

            logger.debug(f"Base path: {base_path}")
            logger.debug(f"Version path: {version_path}")

            # Handle keep_last logic
            if keep_last:
                try:
                    last_key_path = f"{base_path}/last_key.txt"
                    response = self.s3.get_object(
                        Bucket=self.bucket,
                        Key=last_key_path
                    )
                    last_version = response['Body'].read().decode('utf-8')
                    logger.debug(f"Found last version: {last_version}")

                    # Delete previous version
                    self._delete_version(last_version)
                except self.s3.exceptions.NoSuchKey:
                    logger.debug("No previous version found")
                    pass

            # Store chunks
            full_path = f"{base_path}/{version_path}"
            chunks = np.array_split(df, max(1, len(df) // self.chunk_size))
            logger.debug(f"Split DataFrame into {len(chunks)} chunks")

            for i, chunk_df in enumerate(chunks):
                chunk_id = str(uuid.uuid4())
                chunk_path = self._generate_chunk_path(full_path, chunk_id)
                logger.debug(f"Storing chunk {i + 1}/{len(chunks)} at {chunk_path}")

                self.store_chunk(chunk_df, chunk_path)
                logger.debug(f"Successfully stored chunk {i + 1}")

                metadata['chunks'].append({
                    'path': chunk_path,
                    'rows': len(chunk_df),
                    'number': i + 1
                })

            # Update last_key.txt if keep_last
            if keep_last:
                last_key_path = f"{base_path}/last_key.txt"
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=last_key_path,
                    Body=version_path.encode('utf-8')
                )
                logger.debug(f"Updated last_key.txt with {version_path}")

            # Store metadata in DynamoDB
            logger.debug("Storing metadata in DynamoDB")
            self._store_metadata(user_id, df_name, metadata)
            logger.debug("Successfully stored metadata")

            return metadata

        except Exception as e:
            logger.error(f"Error storing dataframe: {str(e)}")
            logger.exception("Full traceback:")
            raise

    def _delete_version(self, version_path: str) -> None:
        """Delete all files in a version"""
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=version_path
            )
            
            for obj in response.get('Contents', []):
                self.s3.delete_object(
                    Bucket=self.bucket,
                    Key=obj['Key']
                )
        except Exception as e:
            logger.warning(f"Error deleting version: {str(e)}")

    def get_dataframe(
        self,
        user_id: str,
        df_name: str,
        external_key: Optional[str] = None,
        use_last: bool = False
    ) -> pd.DataFrame:
        """Retrieve DataFrame with versioning support"""
        try:
            # Get metadata from DynamoDB
            response = self.table.get_item(
                Key={
                    'user_id': user_id,
                    'df_path': df_name
                }
            )
            
            if 'Item' not in response:
                raise ValueError(f"DataFrame {df_name} not found")
                
            metadata = response['Item']['metadata']
            
            # Determine which version(s) to read
            base_path = f"{df_name}/external_key"
            if external_key:
                base_path += f"/{external_key if external_key != 'NOW' else 'default'}"
            
            if use_last:
                try:
                    response = self.s3.get_object(
                        Bucket=self.bucket,
                        Key=f"{base_path}/last_key.txt"
                    )
                    version_path = response['Body'].read().decode('utf-8')
                    prefix = f"{base_path}/{version_path}"
                except self.s3.exceptions.NoSuchKey:
                    raise ValueError("No last version found")
            else:
                prefix = base_path

            # List all chunks
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                raise ValueError(f"No data found for {df_name}")

            # Read and concatenate chunks
            dfs = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv.gz'):
                    response = self.s3.get_object(
                        Bucket=self.bucket,
                        Key=obj['Key']
                    )
                    
                    with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as gz:
                        chunk_df = pd.read_csv(gz)
                        dfs.append(chunk_df)

            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        except Exception as e:
            logger.error(f"Error retrieving dataframe: {str(e)}")
            raise

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def upload(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    try:
        # Add debug logging
        logger.debug("Received upload request")
        logger.debug(f"Event: {json.dumps(event)}")
        
        # Verify auth claims exist
        if 'authorizer' not in event.get('requestContext', {}) or \
           'claims' not in event['requestContext']['authorizer']:
            logger.error("Missing authorization claims")
            return {
                'statusCode': 401,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Unauthorized - missing claims'})
            }

        user_id = event['requestContext']['authorizer']['claims']['sub']
        bucket_name = f"df-{user_id}"
        storage = DataFrameStorage(bucket_name)
        
        # Parse and validate request body
        if not event.get('body'):
            raise ValueError("Missing request body")
            
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in request body: {e}")
            raise ValueError("Invalid JSON in request body")

        # Log received data
        logger.debug(f"Received body: {body}")
        
        # Validate required fields
        if 'dataframe' not in body:
            raise ValueError("Missing 'dataframe' in request")
        if 'dataframe_name' not in body:
            raise ValueError("Missing 'dataframe_name' in request")
            
        # Parse DataFrame
        try:
            df = pd.read_json(body['dataframe'])
        except Exception as e:
            logger.error(f"Error parsing DataFrame: {e}")
            raise ValueError(f"Invalid DataFrame format: {str(e)}")
            
        df_name = body['dataframe_name']
        columns_keys = body.get('columns_keys', {})
        external_key = body.get('external_key', 'NOW')
        keep_last = body.get('keep_last', False)
        
        # Log processing details
        logger.debug(f"Processing DataFrame with shape: {df.shape}")
        logger.debug(f"DataFrame name: {df_name}")
        logger.debug(f"Columns keys: {columns_keys}")
        
        # Store dataframe
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
        
    except ValueError as e:
        logger.warning(f"Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
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
        logger.debug(f"Event: {json.dumps(event)}")
        user_id = event['requestContext']['authorizer']['claims']['sub']
        bucket_name = f"df-{user_id}"
        storage = DataFrameStorage(bucket_name)
        
        # Parse request parameters and handle path with slashes
        df_name = event['pathParameters']['name']
        # URL decode the name parameter
        df_name = unquote(df_name)  # Changed to urllib.parse.unquote
        
        logger.debug(f"Getting DataFrame: {df_name}")
        
        query_params = event.get('queryStringParameters', {}) or {}
        external_key = query_params.get('external_key')
        use_last = query_params.get('use_last', '').lower() == 'true'
        
        # Get dataframe
        df = storage.get_dataframe(
            user_id,
            df_name,
            external_key,
            use_last
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': df.to_json(orient='records')
        }
        
    except ValueError as e:
        logger.warning(f"ValueError: {str(e)}")
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        logger.exception("Error in get handler")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': json.dumps({'error': str(e)})
        }
