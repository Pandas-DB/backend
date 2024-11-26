#!/bin/bash

# Clean up any previous builds
rm -rf layer/python

# Create the correct directory structure
mkdir -p layer/python

# Install dependencies directly into the python directory
pip install \
    aws-lambda-powertools \
    aws-xray-sdk \
    requests \
    boto3 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    pyarrow==12.0.1 \
    aiofiles \
    nest-asyncio \
    pydantic \
    -t layer/python \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.9 \
    --only-binary=:all: \
    --upgrade

# Remove unnecessary files
cd layer/python
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name "*.dist-info" -exec rm -rf {} +
find . -type d -name "*.egg-info" -exec rm -rf {} +
find . -type d -name "tests" -exec rm -rf {} +
find . -type d -name "test" -exec rm -rf {} +
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
find . -name "*.pyd" -delete
find . -name "*.so" -exec strip {} + 2>/dev/null || true
cd ../..
