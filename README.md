# PandasDB Backend

Serverless AWS infrastructure for storing and managing pandas DataFrames with automatic versioning, chunking, and optimized storage patterns.

At high level what it builds is the AWS Cloud (orange boxes):

![Architecture Overview](img/high_level_schema.png)

In more detail about what deploys in AWS and the complete flow follows:

![Architecture Overview](img/low_level_schema.png)

## Overview

PandasDB Backend provides the serverless infrastructure required by the [pandas-db-sdk](https://github.com/yourusername/pandas-db-sdk) package. It handles DataFrame storage, versioning, and retrieval through a secure API built on AWS services.

## Architecture

The system is built on AWS serverless architecture with the following components:

- **API Gateway**: RESTful API endpoints
- **Lambda Functions**: Data processing and storage management
- **S3**: DataFrame storage with automatic chunking
- **DynamoDB**: Metadata and indexing
- **Cognito**: Authentication and user management

![Data Flow](img/dataflow.png)

## Storage Patterns

### 1. Date-based Partitioning
```
bucket/
└── {dataframe_name}/
    └── {date_column}/
        └── 2024-01-01/
            └── {chunk_uuid}.csv.gz
```

### 2. ID-based Partitioning
```
bucket/
└── {dataframe_name}/
    └── {id_column}/
        └── from_{min_id}_to_{max_id}/
            └── {chunk_uuid}.csv.gz
```

### 3. Version Control
```
bucket/
└── {dataframe_name}/
    └── external_key/
        └── default/
            ├── YYYY-MM-DD/
            │   └── HH:MM:SS_{chunk_uuid}.csv.gz
            └── last_key.txt
```

## API Endpoints

### Upload DataFrame
```
POST /dataframes/upload
```
Parameters:
- `dataframe`: CSV or JSON data
- `dataframe_name`: Storage path
- `columns_keys`: Partitioning configuration
- `external_key`: Version identifier
- `keep_last`: Version retention flag

### Get DataFrame
```
GET /dataframes/{name}
```
Parameters:
- `name`: DataFrame path
- `external_key`: (optional) Version filter
- `use_last`: (optional) Latest version flag

## Deployment

### Prerequisites
- AWS Account
- AWS CLI configured
- Node.js ≥ 14.x
- Serverless Framework

### Installation

1. Clone repository:
```bash
git clone https://github.com/yourusername/pandasdb-backend.git
cd pandasdb-backend
```

2. Install dependencies:
```bash
npm install
```

3. Deploy:
```bash
serverless deploy --stage prod
```

### Environment Variables

Create a `.env` file with:
```env
STAGE=prod
REGION=us-east-1
```

## User Creation

No users are created automatically when deploying the backend. Here are the ways to create users:

### 1. Using AWS Console (Easiest)

1. Go to AWS Console → Amazon Cognito → User Pools
2. Click on your user pool (named like `dataframe-storage-system-user-pool-dev`)
3. Go to "Users" tab and click "Create user"
4. Fill in the form:
   - Username: (your email)
   - Email: (same email)
   - Option 1: Check "Send an email invitation" (user will receive temporary password)
   - Option 2: Set a temporary password manually
5. Click "Create user"
6. If you set a temporary password, the user will be prompted to change it on first login

### 2. Using AWS CLI

```bash
# Replace values with your configuration
aws cognito-idp admin-create-user \
  --user-pool-id YOUR_USER_POOL_ID \
  --username admin@example.com \
  --temporary-password YourTempPass123! \
  --user-attributes Name=email,Value=admin@example.com

# Set permanent password (skip force change password)
aws cognito-idp admin-set-user-password \
  --user-pool-id YOUR_USER_POOL_ID \
  --username admin@example.com \
  --password YourPermanentPass123! \
  --permanent
```

### 3. Using Provided Script

The repository includes a script to create users. First, get your User Pool ID:

```bash
# Option 1: From CloudFormation outputs
USER_POOL_ID=$(aws cloudformation describe-stacks \
  --stack-name your-stack-name \
  --query 'Stacks[0].Outputs[?OutputKey==`UserPoolId`].OutputValue' \
  --output text)

# Option 2: List all user pools
aws cognito-idp list-user-pools --max-results 20

# Option 3: Get from AWS Console
# Go to AWS Console → Cognito → User Pools → Your Pool → Pool details
```

Then create a user:

```bash
python scripts/create_initial_user.py \
  --username admin@example.com \
  --password YourPass123! \
  --user-pool-id $USER_POOL_ID \
  --region eu-west-1  # Optional, defaults to eu-west-1
```

### Password Requirements

By default, passwords must meet these requirements:
- Minimum length of 8 characters
- At least 1 number
- At least 1 special character
- At least 1 uppercase letter
- At least 1 lowercase letter

### Finding Your User Pool ID

Several ways to find your User Pool ID:

1. AWS Console:
   - Go to Cognito → User Pools
   - Click on your pool
   - The ID is shown in "Pool details"

2. AWS CLI:
```bash
aws cognito-idp list-user-pools --max-results 20
```

3. CloudFormation outputs:
```bash
aws cloudformation describe-stacks \
  --stack-name your-stack-name \
  --query 'Stacks[0].Outputs[?OutputKey==`UserPoolId`].OutputValue' \
  --output text
```

### Verifying Users

List users in your pool:
```bash
aws cognito-idp list-users --user-pool-id YOUR_USER_POOL_ID
```

Get specific user details:
```bash
aws cognito-idp admin-get-user \
  --user-pool-id YOUR_USER_POOL_ID \
  --username user@example.com
```

## Security

- User isolation through separate S3 buckets
- Cognito authentication
- Fine-grained IAM policies
- CORS configuration
- Request validation

![Security Overview](img/security.png)

## Monitoring

The infrastructure includes:
- CloudWatch Logs
- Lambda metrics
- API Gateway metrics
- S3 access logs
- DynamoDB metrics

![Monitoring Dashboard](img/monitoring.png)

## Development

### Local Testing
```bash
# Install dependencies
npm install

# Run local API
serverless offline start

# Run tests
npm test
```

### Adding New Features

1. Create new Lambda function in `handlers/`
2. Update `serverless.yml`
3. Add tests in `tests/`
4. Update documentation

## Project Structure
```
pandasdb-backend/
├── handlers/
│   ├── dataframes.py
│   └── utils.py
├── tests/
│   └── test_handlers.py
├── img/
│   ├── architecture.png
│   ├── dataflow.png
│   ├── security.png
│   └── monitoring.png
├── serverless.yml
├── requirements.txt
└── README.md
```

## Contributing

1. Fork repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Related Projects

- [pandas-db-sdk](https://github.com/yourusername/pandas-db-sdk): Python SDK for interacting with this backend
- [pandas](https://github.com/pandas-dev/pandas): Data analysis library

## Support

For support:
- Open an issue
- Contact maintainers
- Check documentation

## Authors

- Your Name ([@yourgithub](https://github.com/yourusername))

## Acknowledgments

- AWS Serverless Framework
- Pandas Development Team
- Contributors
