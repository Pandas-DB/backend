import os
from typing import Dict, Any, List
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
import json
from urllib.parse import unquote

from .utils.storage import S3Storage

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def list_dates(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    try:
        if 'authorizer' not in event.get('requestContext', {}) or \
                'claims' not in event['requestContext']['authorizer']:
            raise Exception("Unauthorized")

        user_id = event['requestContext']['authorizer']['claims']['sub']
        df_name = unquote(event['pathParameters']['name'])
        params = event.get('queryStringParameters', {}) or {}
        column = params.get('column')

        if not column:
            raise Exception("Missing required parameter: column")

        bucket_name = f"df-{user_id}"
        prefix = f"{df_name}/data/{column}/"

        # Key change 1: Use S3Storage instead of direct s3 client
        storage = S3Storage(bucket_name)

        # Key change 2: Use list() method from S3Storage which can be used with batch operations
        all_keys = storage.list(prefix)

        # Key change 3: Process unique dates from the complete key list
        dates: List[str] = []
        for key in all_keys:
            try:
                date = key.split('/')[-2]
                if date not in dates:
                    dates.append(date)
            except:
                continue

        dates.sort()

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'dates': dates})
        }

    except Exception as e:
        logger.exception("Error in list_dates handler")
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': str(e)})
        }
