# handlers/event_consumer.py
import json
import os
import boto3
import time

from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError
from typing import List, Dict, Any
from boto3.dynamodb.conditions import Attr
from io import StringIO
import pandas as pd

from .utils.exceptions import LockAcquisitionError

logger = Logger()
tracer = Tracer()

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
LOCK_TABLE = os.environ['LOCK_TABLE']


class Lock:
    def __init__(self, lock_key: str, ttl_seconds: int = 60):
        self.table = dynamodb.Table(LOCK_TABLE)
        self.lock_key = lock_key
        self.ttl_seconds = ttl_seconds
        self.owner = f"lambda-{time.time()}-{os.getenv('AWS_REQUEST_ID', '')}"

    def acquire(self, max_retries: int = 3, retry_delay: float = 1.0) -> bool:
        expires_at = int(time.time()) + self.ttl_seconds

        for attempt in range(max_retries):
            try:
                self.table.put_item(
                    Item={
                        'lockKey': self.lock_key,
                        'owner': self.owner,
                        'expiresAt': expires_at
                    },
                    ConditionExpression=Attr('lockKey').not_exists() |
                                        Attr('expiresAt').lt(int(time.time()))
                )
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                continue
        return False

    def release(self):
        try:
            self.table.delete_item(
                Key={'lockKey': self.lock_key},
                ConditionExpression=Attr('owner').eq(self.owner)
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise
            logger.warning(f"Failed to release lock {self.lock_key}")


def get_history_key(timestamp: str) -> str:
    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    return f"events/{dt.year}/{dt.month:02d}/{dt.day:02d}/history_{dt.hour:02d}.json"


def append_events_to_s3(bucket: str, key: str, events: List[Dict]) -> None:
    lock = Lock(f"s3-{key}")

    try:
        if not lock.acquire():
            raise LockAcquisitionError(f"Could not acquire lock for {key}")

        try:
            # Convert events to DataFrame
            try:
                new_events_df = pd.DataFrame(events)
            except Exception as e:
                raise ValueError(f"Failed to convert events to DataFrame: {str(e)}")

            # Try to get existing content
            try:
                response = s3.get_object(Bucket=bucket, Key=key)
                existing_content = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    logger.info(f"No existing file found at {key}, creating new one")
                    existing_content = pd.DataFrame()
                else:
                    raise

            # Combine existing and new data
            try:
                if not existing_content.empty:
                    all_events_df = pd.concat([existing_content, new_events_df], ignore_index=True)
                else:
                    all_events_df = new_events_df
            except Exception as e:
                raise ValueError(f"Failed to concatenate DataFrames: {str(e)}")

            # Write back to S3
            try:
                csv_buffer = StringIO()
                all_events_df.to_csv(csv_buffer, index=False)

                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=csv_buffer.getvalue(),
                    ContentType='text/csv'
                )
            except Exception as e:
                raise RuntimeError(f"Failed to write to S3: {str(e)}")

        except Exception as e:
            logger.error(f"Error in append_events_to_s3: {str(e)}", exc_info=True)
            raise

    finally:
        try:
            lock.release()
        except Exception as e:
            logger.warning(f"Failed to release lock: {str(e)}")


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    batch_item_failures = []

    # Process each record in the batch
    for record in event.get('Records', []):
        message_id = record['messageId']
        try:
            # Extract message attributes
            try:
                user_id = record['messageAttributes']['UserId']['stringValue']
                bucket = f"df-{user_id}"
            except KeyError as e:
                logger.error(f"Missing required message attribute: {str(e)}")
                batch_item_failures.append({"itemIdentifier": message_id})
                continue

            # Parse message body
            try:
                body = json.loads(record['body'])
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message body: {str(e)}")
                batch_item_failures.append({"itemIdentifier": message_id})
                continue

            # Validate message contents
            key = body.get('key')
            if not key:
                logger.error("Missing 'key' in message body")
                batch_item_failures.append({"itemIdentifier": message_id})
                continue

            if not body.get('data'):
                logger.error("Missing 'data' in message body")
                batch_item_failures.append({"itemIdentifier": message_id})
                continue

            # Append data.csv if not already present
            if not key.endswith('.csv'):
                key = f"{key.rstrip('/')}/data.csv"

            try:
                # Process the event data
                append_events_to_s3(
                    events=[body['data']],
                    key=key,
                    bucket=bucket
                )
                logger.info(f"Successfully processed message {message_id} for bucket {bucket} and key {key}")

            except LockAcquisitionError as e:
                # Lock acquisition failure - message will return to queue
                logger.warning(f"Lock acquisition failed for {key}: {str(e)}")
                batch_item_failures.append({"itemIdentifier": message_id})
            except Exception as e:
                # Other processing errors
                logger.error(f"Error processing data: {str(e)}", exc_info=True)
                batch_item_failures.append({"itemIdentifier": message_id})

        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected error processing message {message_id}: {str(e)}", exc_info=True)
            batch_item_failures.append({"itemIdentifier": message_id})

    logger.info(f"Batch processing complete. {len(batch_item_failures)} messages failed.")

    return {
        "batchItemFailures": batch_item_failures
    }

