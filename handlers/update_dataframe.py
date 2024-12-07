# handlers/event_producer.py
import json
import boto3
from os import environ
from requests.utils import unquote
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from typing import Dict, Any
import pandas as pd
from io import StringIO

from .utils.validators import validate_request
from .utils.utils import create_response

logger = Logger()
tracer = Tracer()


def append_directly_to_s3(key: str, new_data: Dict, bucket: str) -> Dict:
    """Directly append data to existing file in S3"""
    s3 = boto3.client('s3')

    try:
        # Convert new data to DataFrame
        new_df = pd.DataFrame([new_data])

        try:
            logger.info(f'Get existing file for {key}')
            # Try to get existing file
            response = s3.get_object(Bucket=bucket, Key=key)
            existing_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            logger.info(f'Existing data lenght: {len(existing_df)}')

            # Concatenate DataFrames
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            logger.info(f'Combined shape: {combined_df.shape}')
        except s3.exceptions.NoSuchKey:
            # If file doesn't exist, use only new data
            combined_df = new_df
            logger.info('No existing file, using only new data')

        # Save back to S3
        csv_buffer = StringIO()
        combined_df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data appended successfully',
                'file_key': key
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
    except Exception as e:
        logger.exception(f'Error appending to S3: {str(e)}')
        raise


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

        # Check if this is a streaming request
        is_streaming = body.get('stream', False)

        path_params = event.get('pathParameters', {})
        # URL decode the key and ensure it has the right extension
        key = unquote(path_params.get('dataframe_id', ''))

        if not key:
            return create_response(400, {'error': 'Key parameter is required'})

        # Append data.csv if not already present
        if not key.endswith('.csv'):
            key = f"{key.rstrip('/')}/data.csv"

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

        if is_streaming:
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
        else:
            # Direct append to S3
            return append_directly_to_s3(
                bucket=f"df-{user_id}",
                key=key,
                new_data=body['data'],
            )

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
