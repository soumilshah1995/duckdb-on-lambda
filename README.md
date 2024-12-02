# duckdb-on-lambda
duckdb-on-lambda
![image](https://github.com/user-attachments/assets/c5b5d4eb-cd4f-42a3-89fe-9dfa452d9909)


# Steps to test it locally 
```
bash /Users/soumilshah/IdeaProjects/clickhoue-examples/duckdb/duckdb-lambdas/upload_data.sh

docker build -t duckdb-lambda .

docker run -p 9000:8080 \
    -v ~/.aws:/root/.aws:ro \
    duckdb-lambda

python3 test_duckdb_lambda.py
```
