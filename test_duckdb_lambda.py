import unittest
import requests
import json
import os

class TestLambdaFunction(unittest.TestCase):
    BASE_URL = "http://localhost:9000/2015-03-31/functions/function/invocations"
    
    # Retrieve the bucket name from environment variables or set a default
    ICEBERG_BUCKET = os.getenv('ICEBERG_BUCKET', '<BUCKET_NAME>')
    DELTA_BUCKET = os.getenv('DELTA_BUCKET', '<BUCKET_NAME>')
    HUDI_BUCKET = os.getenv('HUDI_BUCKET', '<BUCKET_NAME>')

    def test_iceberg_query(self):
        """Test the Lambda function with an Iceberg query."""
        payload = {
            "query": f"SELECT count(*) FROM iceberg_scan(\"s3://{self.ICEBERG_BUCKET}/datalake/default.db/user_events/metadata/00003-19789543-c6d4-43ac-855b-41c3268b34a0.metadata.json\")",
            "extensions": ["iceberg", "aws", "httpfs"]
        }
        response = requests.post(self.BASE_URL, json=payload)
        self.assertEqual(response.status_code, 200)
        response_body = response.json()
        print("Iceberg Query Response:", response_body)

    def test_parquet_query(self):
        """Test the Lambda function with a Parquet query."""
        payload = {
            "query": "SELECT avg(c_acctbal) FROM read_parquet(\"https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet\")",
            "extensions": []
        }
        response = requests.post(self.BASE_URL, json=payload)
        self.assertEqual(response.status_code, 200)
        response_body = response.json()
        print("Parquet Query Response:", response_body)

    def test_delta_query(self):
        """Test the Lambda function with a Delta query."""
        payload = {
            "query": f"SELECT count(*) FROM delta_scan(\"s3://{self.DELTA_BUCKET}/datalake/delta/\");",
            "extensions": ["delta", "aws", "httpfs"]
        }
        response = requests.post(self.BASE_URL, json=payload)
        self.assertEqual(response.status_code, 200)
        response_body = response.json()
        print("Delta Query Response:", response_body)

    def test_hudi_query(self):
        from handler import lambda_handler
        """Test the Lambda function with a Hudi query."""
        event = {
            "query": "SELECT COUNT(*) FROM arrow_table",
            "extensions": [],
            "hudi": f"s3://{self.HUDI_BUCKET}/datalake/hudi/"
        }

        response = lambda_handler(event, context={})
        print("Hudi Response", response)
        self.assertEqual(response.get("statusCode"), 200)


if __name__ == '__main__':
    unittest.main()
