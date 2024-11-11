#!/bin/bash

# Clean up any previous builds
rm -rf layer/pandas/python
rm -rf layer/common/python
mkdir -p layer/pandas/python/lib/python3.9/site-packages
mkdir -p layer/common/python/lib/python3.9/site-packages

# Create requirements files
cat > layer/requirements-common.txt << EOL
aws-lambda-powertools
boto3
aiofiles
nest-asyncio
EOL

cat > layer/requirements-pandas.txt << EOL
pandas==2.0.3
numpy==1.24.3
pyarrow==12.0.1
EOL

# Create Dockerfile for pandas layer with aggressive optimization
cat > layer/Dockerfile.pandas << EOL
FROM public.ecr.aws/lambda/python:3.9

COPY requirements-pandas.txt .
RUN pip install --no-cache-dir -r requirements-pandas.txt -t /python/lib/python3.9/site-packages/ && \
    cd /python/lib/python3.9/site-packages/ && \
    find . -type d -name "tests" -exec rm -rf {} + && \
    find . -type d -name "test" -exec rm -rf {} + && \
    find . -type f -name "*.pyc" -delete && \
    find . -type f -name "*.pyo" -delete && \
    find . -type f -name "*.pyd" -delete && \
    find . -type d -name "__pycache__" -exec rm -rf {} + && \
    find . -type f -name "*.so" -exec strip {} + 2>/dev/null || true && \
    find . -type f -name "*.c" -delete && \
    find . -type f -name "*.h" -delete && \
    rm -rf pandas/tests && \
    rm -rf numpy/tests && \
    rm -rf numpy/doc && \
    rm -rf pyarrow/tests && \
    find . -name "*.html" -delete && \
    find . -name "*.txt" -delete
EOL

# Create Dockerfile for common dependencies
cat > layer/Dockerfile.common << EOL
FROM public.ecr.aws/lambda/python:3.9

COPY requirements-common.txt .
RUN pip install --no-cache-dir -r requirements-common.txt -t /python/lib/python3.9/site-packages/ && \
    cd /python/lib/python3.9/site-packages/ && \
    find . -type d -name "tests" -exec rm -rf {} + && \
    find . -type d -name "test" -exec rm -rf {} + && \
    find . -type f -name "*.pyc" -delete && \
    find . -type f -name "*.pyo" -delete && \
    find . -type f -name "*.pyd" -delete && \
    find . -type d -name "__pycache__" -exec rm -rf {} + && \
    find . -type f -name "*.so" -exec strip {} + 2>/dev/null || true
EOL

# Build pandas layer
cd layer
echo "Building pandas layer..."
docker build -t lambda-layer-pandas -f Dockerfile.pandas .
docker run --rm -v $(pwd)/pandas/python:/python lambda-layer-pandas cp -r /python/lib/python3.9/site-packages/* /python/

# Build common layer
echo "Building common layer..."
docker build -t lambda-layer-common -f Dockerfile.common .
docker run --rm -v $(pwd)/common/python:/python lambda-layer-common cp -r /python/lib/python3.9/site-packages/* /python/

cd ..
