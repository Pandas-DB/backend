import boto3
import json
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from requests.utils import unquote
import uuid
from typing import Dict, Any
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
import pandas as pd

from .utils.validators import validate_request
from .utils.exceptions import ValidationError

logger = Logger()
tracer = Tracer()


class S3SelectError(Exception):
    """Custom exception for S3 Select operations"""
    pass


def create_temp_key() -> str:
    """Generate a unique temporary file key"""
    date_prefix = datetime.now().strftime('%Y%m%d')
    unique_id = str(uuid.uuid4())
    return f"temp/{date_prefix}/{unique_id}.csv"


def schedule_cleanup(bucket: str, key: str, s3_client) -> None:
    """Schedule cleanup of temporary file"""
    try:
        expiration_time = datetime.now() + timedelta(hours=1)
        cleanup_event = {
            'bucket': bucket,
            'key': key,
            'delete_after': expiration_time.isoformat()
        }

        s3_client.put_events(
            Entries=[{
                'Source': 'custom.s3select',
                'DetailType': 'cleanup',
                'Detail': json.dumps(cleanup_event),
                'EventBusName': 'default'
            }]
        )
    except Exception as e:
        logger.warning(f"Failed to schedule cleanup: {str(e)}")
        # Continue execution as this is not critical


def get_column_names(s3, bucket: str, key: str) -> list:
    """Get column names from Parquet file using S3 Select"""
    try:
        resp = s3.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression="SELECT * FROM s3object LIMIT 1",
            InputSerialization={
                'Parquet': {},
                'CompressionType': 'NONE',
            },
            OutputSerialization={
                'JSON': {
                    'RecordDelimiter': '\n'
                }
            }
        )

        # Process the response to get the first record
        for event in resp['Payload']:
            if 'Records' in event:
                json_str = event['Records']['Payload'].decode('utf-8')
                record = json.loads(json_str)
                return list(record.keys())

        return []  # Return empty list if no records found
    except Exception as e:
        logger.error(f"Failed to get column names: {str(e)}")
        return []


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def get_dataframe(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """Main Lambda handler for S3 Select streaming operations"""
    user_id = validate_request(event)
    logger.info(f"Validated user_id: {user_id}")

    s3 = boto3.client('s3')
    bucket = f"df-{user_id}"
    logger.info(f"Using bucket: {bucket}")

    # Initialize variables for cleanup in case of failure
    multipart_upload = None
    temp_key = None

    try:
        # Get the key from path parameters instead of body
        path_params = event.get('pathParameters', {})
        logger.info(f"Path parameters: {path_params}")

        if path_params is None:
            logger.error("Path parameters is None")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing path parameters'}),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }

        key = unquote(path_params.get('dataframe_id', ''))

        if not key:
            raise ValidationError("Key parameter is required")

        # Append data.parquet if not already present
        if not key.endswith('.parquet'):
            key = f"{key.rstrip('/')}/data.parquet"
        logger.info(f"Final key: {key}")

        # Extract and validate parameters
        query_params = event.get('queryStringParameters') or {}
        query = query_params.get('query') if query_params else 'SELECT * FROM s3object'
        if not query:  # Handle empty string case
            query = 'SELECT * FROM s3object'

        logger.info(f"Processing query: {query} on {bucket}/{key}")

        try:
            s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"File not found: {bucket}/{key}")
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': f'File not found: {key}'}),
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                }
            logger.error(f"S3 error checking file: {str(e)}")
            raise

        # Get column names first
        column_names = get_column_names(s3, bucket, key)
        logger.info(f"Retrieved columns: {column_names}")

        # Generate temporary file key
        temp_key = create_temp_key()

        # Initialize multipart upload
        multipart_upload = s3.create_multipart_upload(
            Bucket=bucket,
            Key=temp_key,
            ContentType='text/csv'
        )

        # Execute S3 Select query
        resp = s3.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=query,
            InputSerialization={
                'Parquet': {},
                'CompressionType': 'NONE',
            },
            OutputSerialization={'CSV': {}}
        )
        logger.info('Get query done!')

        parts = []
        part_number = 1
        current_part = BytesIO()
        bytes_processed = 0

        if column_names:
            header_row = ','.join(f'"{col}"' for col in column_names) + '\n'
            current_part.write(header_row.encode('utf-8'))
            bytes_processed += len(header_row)

        for event_dict in resp['Payload']:
            if 'Records' in event_dict:
                data = event_dict['Records']['Payload']
                current_part.write(data)
                bytes_processed += len(data)

                # Upload part when it reaches threshold (~5MB)
                if current_part.tell() >= 5 * 1024 * 1024:
                    logger.info(f'data size: {current_part.tell()}')
                    current_part.seek(0)

                    part = s3.upload_part(
                        Body=current_part.getvalue(),
                        Bucket=bucket,
                        Key=temp_key,
                        PartNumber=part_number,
                        UploadId=multipart_upload['UploadId']
                    )

                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })

                    logger.info(f"Uploaded part {part_number}, size: {current_part.tell()} bytes")
                    part_number += 1
                    current_part = BytesIO()

        # Upload final part if any data remains
        if current_part.tell() > 0:
            current_part.seek(0)
            part = s3.upload_part(
                Body=current_part.getvalue(),
                Bucket=bucket,
                Key=temp_key,
                PartNumber=part_number,
                UploadId=multipart_upload['UploadId']
            )

            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })

            logger.info(f"Uploaded final part {part_number}, size: {current_part.tell()} bytes")

        # Complete multipart upload
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=temp_key,
            UploadId=multipart_upload['UploadId'],
            MultipartUpload={'Parts': parts}
        )

        logger.info(f"Completed multipart upload, total parts: {len(parts)}")

        # Generate pre-signed URL
        url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': temp_key
            },
            ExpiresIn=3600  # 1 hour
        )

        logger.info(f"Prepared presigned URL, last 5 characters: {url[-5:]}")

        # Schedule cleanup
        schedule_cleanup(bucket, temp_key, s3)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'download_url': url,
                'expires_in': 3600,
                'query': query,
                'bytes_processed': bytes_processed,
                'columns': column_names  # Include columns in response
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    except ValueError as e:
        logger.error(f"Invalid parameters: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    except ClientError as e:
        logger.error(f"AWS service error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal service error'}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    finally:
        # Cleanup on failure
        if multipart_upload and 'parts' not in locals():
            try:
                s3.abort_multipart_upload(
                    Bucket=bucket,
                    Key=temp_key,
                    UploadId=multipart_upload['UploadId']
                )
                logger.info("Aborted multipart upload due to failure")
            except Exception as e:
                logger.error(f"Failed to abort multipart upload: {str(e)}")
