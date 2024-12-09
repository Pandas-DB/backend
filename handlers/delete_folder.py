import boto3
from typing import Dict, Any
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from botocore.exceptions import ClientError
import json
from requests.utils import unquote

from .utils.validators import validate_request
from .utils.exceptions import ValidationError

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def delete_folder(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """Lambda handler for S3 folder deletion"""
    user_id = validate_request(event)
    logger.info(f"Validated user_id: {user_id}")

    s3 = boto3.client('s3')
    bucket = f"df-{user_id}"

    try:
        # Get the folder path from path parameters
        path_params = event.get('pathParameters', {})
        if not path_params:
            raise ValidationError("Missing path parameters")

        folder_path = unquote(path_params.get('dataframe_id', ''))
        if not folder_path:
            raise ValidationError("Folder path is required")

        # Ensure path ends with '/'
        if not folder_path.endswith('/'):
            folder_path += '/'

        logger.info(f"Deleting all dataframes and sub-folders in {bucket}/{folder_path}")

        # List and delete all objects with the prefix
        paginator = s3.get_paginator('list_objects_v2')
        objects_deleted = 0

        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=folder_path):
                if 'Contents' in page:
                    # Prepare objects for deletion
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]

                    if objects:
                        # Delete objects in batches of 1000 (S3 limit)
                        for i in range(0, len(objects), 1000):
                            batch = objects[i:i + 1000]
                            s3.delete_objects(
                                Bucket=bucket,
                                Delete={
                                    'Objects': batch,
                                    'Quiet': True
                                }
                            )
                            objects_deleted += len(batch)
                            logger.info(f"Deleted {len(batch)} objects")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Successfully deleted {objects_deleted} objects from {folder_path}',
                    'objects_deleted': objects_deleted
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }

        except ClientError as e:
            logger.error(f"Failed to delete objects: {str(e)}")
            raise

    except ValidationError as e:
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
