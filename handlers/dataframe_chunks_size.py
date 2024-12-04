import json
from typing import Dict, Any, List, Optional, Tuple
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from urllib.parse import unquote

from .utils.config import StorageConfig
from .utils.storage import S3Storage
from .utils.validators import validate_request

logger = Logger()
tracer = Tracer()


class DataFrameChunksSize:
    def __init__(self, bucket_name: str, config: Optional[StorageConfig] = None):
        self.storage = S3Storage(bucket_name)
        self.config = config or StorageConfig()

    def _list_relevant_keys(self, prefix: str, partition_info: Dict[str, Any]) -> List[str]:
        """List all relevant S3 keys based on partition type and filters."""
        all_keys = self.storage.list(prefix)

        if not partition_info or 'partition_type' not in partition_info:
            return []

        partition_type = partition_info['partition_type'].lower()
        if partition_type != 'date':
            return []

        if 'column' in partition_info:
            prefix = f"{prefix}/data/{partition_info['column']}"
            all_keys = self.storage.list(prefix)

        relevant_keys = []
        for key in all_keys:
            try:
                path_parts = key.split('/')
                if len(path_parts) >= 2:
                    key_date = path_parts[-2]

                    if not partition_info.get('start_date') and not partition_info.get('end_date'):
                        relevant_keys.append(key)
                        continue

                    is_relevant = True
                    if partition_info.get('start_date'):
                        is_relevant = is_relevant and key_date >= partition_info['start_date']
                    if partition_info.get('end_date'):
                        is_relevant = is_relevant and key_date <= partition_info['end_date']

                    if is_relevant:
                        relevant_keys.append(key)
            except Exception as e:
                logger.warning(f"Error processing key {key}: {str(e)}")
                continue

        return relevant_keys

    def get_index_ranges(self, prefix: str, partition_info: Dict[str, Any], max_size_mb: int = 5) -> List[
        Tuple[int, int]]:
        """Get index ranges for chunks that total up to max_size_mb each."""
        keys = sorted(self._list_relevant_keys(prefix, partition_info))
        current_size = 0
        ranges = []
        start_idx = 0

        for idx, key in enumerate(keys):
            try:
                size = self.storage.head(key)
                current_size += size
                if current_size >= max_size_mb * 1024 * 1024:
                    ranges.append((start_idx, idx - 1))
                    start_idx = idx
                    current_size = size
            except Exception as e:
                logger.error(f"Error getting size for {key}: {e}")

        if start_idx < len(keys):
            ranges.append((start_idx, len(keys) - 1))

        return ranges


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def get(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """Lambda handler for getting chunk size ranges."""
    try:
        user_id = validate_request(event)
        storage = DataFrameChunksSize(f"df-{user_id}")

        df_name = unquote(event['pathParameters']['name'])
        params = event.get('queryStringParameters', {}) or {}

        partition_info = {}
        if params.get('partition_type'):
            partition_info = {
                'partition_type': params.get('partition_type'),
                'column': params.get('column'),
                'start_date': params.get('start_date'),
                'end_date': params.get('end_date')
            }
            partition_info = {k: v for k, v in partition_info.items() if v is not None}

        ranges = storage.get_index_ranges(
            prefix=df_name,
            partition_info=partition_info,
            max_size_mb=int(params.get('max_size_mb', 5))
        )

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(ranges)
        }

    except Exception as e:
        logger.exception("Error in get")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
