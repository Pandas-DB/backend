import os
import json
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent

logger = Logger()
tracer = Tracer()

@logger.inject_lambda_context
@tracer.capture_lambda_handler
async def get_config(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """Return Cognito configuration"""
    try:
        config = {
            'userPoolId': os.environ['COGNITO_USER_POOL_ID'],
            'userPoolClientId': os.environ['COGNITO_CLIENT_ID'],
            'region': os.environ['AWS_REGION']
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(config)
        }
    except Exception as e:
        logger.exception("Error in get_config handler")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }

@logger.inject_lambda_context
@tracer.capture_lambda_handler
async def verify_token(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """Verify authentication token"""
    try:
        # Token verification happens automatically via API Gateway authorizer
        # If we get here, token is valid
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'status': 'valid'})
        }
    except Exception as e:
        logger.exception("Error in verify_token handler")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
