# handlers/update_dataframe.py
import json
import boto3
from os import environ
from requests.utils import unquote
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from typing import Dict, Any
import pandas as pd
from io import BytesIO

from .utils.validators import validate_request
from .utils.utils import create_response

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    sqs = boto3.client('sqs')

    try:
        user_id = validate_request(event)

        try:
            body = json.loads(event['body'])
        except (TypeError, json.JSONDecodeError) as e:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid JSON in request body',
                    'details': str(e)
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }

        path_params = event.get('pathParameters', {})
        # URL decode the key and ensure it has the right extension
        key = unquote(path_params.get('dataframe_id', ''))

        if not key:
            return create_response(400, {'error': 'Key parameter is required'})

        # Append data.parquet if not already present
        if not key.endswith('.parquet'):
            key = f"{key.rstrip('/')}/data.parquet"

        if 'data' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required field: data'
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }

        # Send to SQS for streaming processing
        try:
            response = sqs.send_message(
                QueueUrl=environ['EVENTS_QUEUE_URL'],
                MessageBody=json.dumps(body),
                MessageAttributes={
                    'EventType': {
                        'DataType': 'String',
                        'StringValue': 'stream'
                    },
                    'UserId': {
                        'DataType': 'String',
                        'StringValue': user_id
                    }
                }
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Event queued for streaming',
                    'messageId': response['MessageId']
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        except Exception as e:
            logger.exception('Error sending message to SQS')
            raise

    except Exception as e:
        logger.exception('Unexpected error in event producer')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
