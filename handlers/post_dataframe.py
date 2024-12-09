import boto3
import json
from typing import Dict, Any
from requests.utils import unquote
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent

from .utils.validators import validate_request
from .utils.utils import create_response

logger = Logger()
tracer = Tracer()


class UploadError(Exception):
    """Custom exception for upload operations"""
    pass


def handle_init_upload(s3, bucket, key) -> Dict[str, Any]:
    """Handle initialization of multipart upload"""
    try:
        multipart = s3.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType='text/csv'
        )

        logger.info(f"Initialized multipart upload for {bucket}/{key}")

        return create_response(200, {
            'upload_id': multipart['UploadId']
        })

    except ClientError as e:
        logger.error(f"AWS error in init upload: {str(e)}")
        return create_response(500, {'error': 'Failed to initialize upload'})


def handle_get_part_url(s3, bucket, key, upload_id, part_number) -> Dict[str, Any]:
    """Handle generation of pre-signed URL for part upload"""
    try:
        # Generate pre-signed URL
        url = s3.generate_presigned_url(
            'upload_part',
            Params={
                'Bucket': bucket,
                'Key': key,
                'UploadId': upload_id,
                'PartNumber': int(part_number)
            },
            ExpiresIn=3600  # URL valid for 1 hour
        )

        logger.info(f"Generated presigned URL for part {part_number} of {bucket}/{key}")

        return create_response(200, {
            'upload_url': url
        })

    except ClientError as e:
        logger.error(f"AWS error in get part URL: {str(e)}")
        return create_response(500, {'error': 'Failed to generate upload URL'})


def handle_complete_upload(s3, bucket, key, upload_id, parts) -> Dict[str, Any]:
    """Handle completion of multipart upload"""
    try:
        result = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        logger.info(f"Completed multipart upload for {bucket}/{key}")

        return create_response(200, {
            'bucket': bucket,
            'key': key,
            'location': result.get('Location')
        })

    except ClientError as e:
        logger.error(f"AWS error in complete upload: {str(e)}")
        return create_response(500, {'error': 'Failed to complete upload'})
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def post_dataframe(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """Main Lambda handler for multipart upload operations"""
    user_id = validate_request(event)
    s3 = boto3.client('s3')
    bucket = f"df-{user_id}"

    if not bucket:
        return create_response(400, {'error': 'Bucket not specified'})

    try:
        path_params = event.get('pathParameters', {})
        # URL decode the key and ensure it has the right extension
        key = unquote(path_params.get('dataframe_id', ''))

        if not key:
            return create_response(400, {'error': 'Key parameter is required'})

        # Append data.parquet if not already present
        if not key.endswith('.parquet'):
            key = f"{key.rstrip('/')}/data.parquet"

        body = json.loads(event.get('body')) if event.get('body') else None
        upload_id = body.get('upload_id') if body else None
        part_number = body.get('part_number') if body else None
        parts = body.get('parts') if body else None

        logger.info(f'Key: {key} | Upload Id: {upload_id} | Part Number: {part_number}')

        # Case 1: Initial upload request - no upload_id
        if not upload_id:
            return handle_init_upload(s3, bucket, key)

        # Case 2: Get presigned URL for part upload
        if upload_id and part_number:
            return handle_get_part_url(s3, bucket, key, upload_id, part_number)

        # Case 3: Complete multipart upload
        if upload_id and parts:
            return handle_complete_upload(s3, bucket, key, upload_id, parts)

        return create_response(400, {'error': 'Invalid request parameters'})

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})
