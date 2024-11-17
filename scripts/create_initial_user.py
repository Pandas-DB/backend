import boto3
import os
import argparse
from botocore.exceptions import ClientError

def create_cognito_user(
    username: str,
    password: str,
    user_pool_id: str,
    region: str = 'eu-west-1'
) -> None:
    """Create a new user in Cognito User Pool"""
    client = boto3.client('cognito-idp', region_name=region)
    
    try:
        # Create user
        client.admin_create_user(
            UserPoolId=user_pool_id,
            Username=username,
            TemporaryPassword=password,
            UserAttributes=[
                {
                    'Name': 'email',
                    'Value': username
                },
                {
                    'Name': 'email_verified',
                    'Value': 'true'
                }
            ]
        )
        
        # Set permanent password (skip force change password)
        client.admin_set_user_password(
            UserPoolId=user_pool_id,
            Username=username,
            Password=password,
            Permanent=True
        )
        
        print(f"Successfully created user: {username}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'UsernameExistsException':
            print(f"User {username} already exists")
        else:
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create initial Cognito user')
    parser.add_argument('--username', required=True, help='User email/username')
    parser.add_argument('--password', required=True, help='User password')
    parser.add_argument('--user-pool-id', required=True, help='Cognito User Pool ID')
    parser.add_argument('--region', default='eu-west-1', help='AWS region')
    
    args = parser.parse_args()
    
    create_cognito_user(
        username=args.username,
        password=args.password,
        user_pool_id=args.user_pool_id,
        region=args.region
    )
