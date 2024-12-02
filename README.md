# duckdb-on-lambda
duckdb-on-lambda
![image](https://github.com/user-attachments/assets/c5b5d4eb-cd4f-42a3-89fe-9dfa452d9909)

#### Blogs 
https://www.linkedin.com/pulse/fast-cost-effective-querying-duckdb-aws-lambda-docker-soumil-shah-omhke/?trackingId=4dj0%2Fqy%2BSPuT6Kp%2FFoekCw%3D%3D

# Steps to test it locally 
```
bash /Users/soumilshah/IdeaProjects/clickhoue-examples/duckdb/duckdb-lambdas/upload_data.sh

docker build -t duckdb-lambda .

docker run -p 9000:8080 \
    -v ~/.aws:/root/.aws:ro \
    duckdb-lambda

python3 test_duckdb_lambda.py
```
