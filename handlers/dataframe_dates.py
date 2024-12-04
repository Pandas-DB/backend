import os
from typing import Dict, Any, List
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
import json
from urllib.parse import unquote
import boto3

logger = Logger()
tracer = Tracer()

def get_s3_client():
    return boto3.client('s3')

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
        
        s3 = get_s3_client()
        dates: List[str] = []
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
            for prefix_obj in page.get('CommonPrefixes', []):
                date = prefix_obj['Prefix'].split('/')[-2]
                if date not in dates:
                    dates.append(date)
        
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
