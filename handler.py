import json
import os
import duckdb
from hudi import HudiTable
import pyarrow as pa
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Change to logging.WARNING to suppress INFO messages

def execute_duckdb_query(con, query, extensions):
    try:
        # Install and load the necessary extensions
        for ext in extensions:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
            logging.info(f"Installed and loaded extension: {ext}")

        # Create the S3 secret using environment variables
        key_id = os.getenv('AWS_ACCESS_KEY_ID')
        secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        region = os.getenv('AWS_DEFAULT_REGION')

        con.execute(f"""
            CREATE SECRET my_s3_secret (
                TYPE 'S3',
                KEY_ID '{key_id}',
                SECRET '{secret}',
                REGION '{region}'
            );
        """)

        # Load AWS credentials (if necessary)
        con.execute("CALL load_aws_credentials();")

        # Execute the query
        df = con.execute(query).fetchdf()

        # Convert the dataframe to JSON
        result = df.to_json(orient='records')

        return result

    except Exception as e:
        error_message = {'error': str(e)}
        logging.error(f"Error executing query: {error_message}")
        return error_message


def lambda_handler(event, context):
    try:
        query = event.get('query', '')
        extensions = event.get('extensions', [])
        hudi_path = event.get('hudi', None)

        if not query:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Query is required'})
            }

        # Connect to DuckDB in-memory database
        con = duckdb.connect(database=':memory:')

        # Set S3 endpoint and retry configurations
        con.execute("SET s3_endpoint='s3.us-east-1.amazonaws.com';")
        con.execute("SET http_retries = 10;")
        con.execute("SET http_retry_backoff = 5;")

        if hudi_path:
            # Handle Hudi table querying
            hudi_table_cloud = HudiTable(hudi_path)
            records_cloud = hudi_table_cloud.read_snapshot()
            arrow_table = pa.Table.from_batches(records_cloud)

            # Register the Arrow table as a DuckDB relation
            con.register('arrow_table', arrow_table)
            logging.info("Arrow table registered successfully.")

            query_result = execute_duckdb_query(con, query, extensions)

        else:
            # Regular DuckDB query execution
            query_result = execute_duckdb_query(con, query, extensions)

        return {
            'statusCode': 200,
            'body': query_result
        }

    except Exception as e:
        error_message = {'error': str(e)}
        logging.error(f"Unexpected error: {error_message}")

    return {
        'statusCode': 500,
        'body': json.dumps(error_message)
    }
