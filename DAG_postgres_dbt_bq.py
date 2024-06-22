from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'etl_postgres_to_bigquery',
    default_args=default_args,
    description='ETL pipeline from PostgreSQL to BigQuery',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'dbt', 'postgres', 'bigquery']
)

def load_table_to_bq(table_name, destination_table):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    df = pd.read_sql(f'SELECT * FROM public.{table_name}', engine)
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
    job.result()

tables = [
    'categories', 'customers', 'employee_territories', 'employees', 
    'order_details', 'orders', 'products', 'regions', 
    'shippers', 'suppliers', 'territories'
]

for table in tables:
    task = PythonOperator(
        task_id=f'load_{table}_to_bq',
        python_callable=load_table_to_bq,
        op_kwargs={
            'table_name': table,
            'destination_table': f'etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq.{table}'
        },
        dag=dag
    )

def run_dbt_model(model_path, destination_table):
    os.system(f'cd "D:\\Bootcamp data engineer\\Project\\Project 2\\Jawaban\\northwind_project" && dbt run --models {model_path}')
    
    client = bigquery.Client()
    query = f'SELECT * FROM `{destination_table}`'
    job_config = bigquery.QueryJobConfig(destination=destination_table, write_disposition="WRITE_TRUNCATE")
    
    query_job = client.query(query, job_config=job_config)
    query_job.result()

datamart_models = [
    'northwind_project.models.marts.dm_monthly_supplier_gross_revenue',
    'northwind_project.models.marts.dm_top_category_sales',
    'northwind_project.models.marts.dm_top_employee_revenue'
]

for model in datamart_models:
    task = PythonOperator(
        task_id=f'run_{model.split(".")[-1]}_model',
        python_callable=run_dbt_model,
        op_kwargs={
            'model_path': model,
            'destination_table': f'etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq.{model.split(".")[-1]}'
        },
        dag=dag
    )
