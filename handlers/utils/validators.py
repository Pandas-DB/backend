from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from .exceptions import ValidationError


def validate_request(event: APIGatewayProxyEvent) -> str:
    """Validate the request and extract user ID from claims."""
    if 'authorizer' not in event.get('requestContext', {}) or \
            'claims' not in event['requestContext']['authorizer']:
        raise ValidationError("Unauthorized")
    return event['requestContext']['authorizer']['claims']['sub']

