#!/bin/bash

# Define the base local path and S3 bucket path
LOCAL_BASE_PATH="/Users/soumilshah/IdeaProjects/clickhoue-examples/duckdb/duckdb-lambdas/datalake-sample"
S3_BUCKET_PATH="s3://datalake-demo-1995/datalake"

# Function to upload a directory to S3
upload_directory() {
    local local_path="$1"
    local s3_path="$2"

    echo "Uploading $local_path to $s3_path..."
    aws s3 cp "$local_path" "$s3_path" --recursive
}

# Upload each directory
upload_directory "$LOCAL_BASE_PATH/delta" "$S3_BUCKET_PATH/delta/"
upload_directory "$LOCAL_BASE_PATH/hudi" "$S3_BUCKET_PATH/hudi/"
upload_directory "$LOCAL_BASE_PATH/parquet" "$S3_BUCKET_PATH/parquet/"
upload_directory "$LOCAL_BASE_PATH/default.db" "$S3_BUCKET_PATH/default.db/"

echo "All uploads completed."
