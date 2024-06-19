# Pull Data CSV ke PostgreSQL --> Extract (PySpark/Python) --> HDFS + Hive /BigQuery/Snowflake --> Result :(Data Mart) : Orchestration :DAG Airflow
create a virtual env of python first :
```
python3 -m venv /Users/lip13farhanpratama/venv_dskola
source /Users/lip13farhanpratama/venv_dskola/bin/activate
```
1. build the postgres image
```
docker build -t northwind -f Dockerfile.postgres .
```
2. run postgres container
if you want to check the result on local (uncomment first the EXPOSE command on dockerfile postgres)
```
docker run -d -p 5432:5432 --name postgres northwind