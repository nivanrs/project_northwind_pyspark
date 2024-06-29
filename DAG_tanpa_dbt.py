from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Daftar nama tabel
tables = [
    'categories',
    'customers',
    'employee_territories',
    'employees',
    'order_details',
    'orders',
    'products',
    'regions',
    'shippers',
    'suppliers',
    'territories'
]

def fetch_data_from_postgres(table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
    file_path = f'/tmp/{table_name}.csv'
    df.to_csv(file_path, index=False)
    cursor.close()
    connection.close()
    return file_path

def load_csv_to_gcs(table_name, **kwargs):
    from google.cloud import storage
    file_path = kwargs['ti'].xcom_pull(task_ids=f'fetch_data_from_postgres_{table_name}')
    client = storage.Client()
    bucket = client.bucket('project_3_de')
    blob = bucket.blob(f'{table_name}.csv')
    blob.upload_from_filename(file_path)
    os.remove(file_path)  # Optionally remove the file after uploading

def get_table_schema(table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
    columns = cursor.fetchall()
    cursor.close()
    connection.close()
    schema = []
    for column in columns:
        col_name = column[0]
        col_type = column[1]
        if col_type == 'integer':
            schema.append({'name': col_name, 'type': 'INT64', 'mode': 'NULLABLE'})
        elif col_type == 'character varying' or col_type == 'text':
            schema.append({'name': col_name, 'type': 'STRING', 'mode': 'NULLABLE'})
        elif col_type == 'timestamp without time zone' or col_type == 'timestamp with time zone':
            schema.append({'name': col_name, 'type': 'TIMESTAMP', 'mode': 'NULLABLE'})
        # Add more data type mappings as needed
    return schema

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'postgres_to_bigquery_with_datamarts',
    default_args=default_args,
    description='A DAG to pull data from PostgreSQL tables, load to BigQuery, and create datamarts',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Ensure the BigQuery dataset exists
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id='etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq',
        location='asia-southeast2'
    )

    for table in tables:
        fetch_data = PythonOperator(
            task_id=f'fetch_data_from_postgres_{table}',
            python_callable=fetch_data_from_postgres,
            op_kwargs={'table_name': table},
        )

        load_to_gcs = PythonOperator(
            task_id=f'load_csv_to_gcs_{table}',
            python_callable=load_csv_to_gcs,
            op_kwargs={'table_name': table},
            provide_context=True,
        )

        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f'create_bq_table_{table}',
            dataset_id='etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq',
            table_id=table,
            schema_fields=get_table_schema(table),
            location='asia-southeast2'
        )

        load_to_bq = BigQueryInsertJobOperator(
            task_id=f'load_to_bq_{table}',
            configuration={
                "load": {
                    "sourceUris": [f'gs://project_3_de/{table}.csv'],
                    "destinationTable": {
                        "projectId": 'etl-use-airflow-fr-hdfs-to-bq',
                        "datasetId": 'etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq',
                        "tableId": table
                    },
                    "sourceFormat": "CSV",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            }
        )

        create_dataset >> fetch_data >> load_to_gcs >> create_table >> load_to_bq

    # Datamart: dm_monthly_supplier_gross_revenue
    create_dm_monthly_supplier_gross_revenue = BigQueryInsertJobOperator(
        task_id='create_dm_monthly_supplier_gross_revenue',
        configuration={
            "query": {
                "query": '''
                    WITH order_details AS (
                        SELECT
                            od.ORDERID,
                            od.PRODUCTID,
                            od.UNITPRICE,
                            od.QUANTITY,
                            od.DISCOUNT,
                            (od.UNITPRICE * (1 - od.DISCOUNT) * od.QUANTITY) AS gross_revenue
                        FROM `etl-use-airflow-fr-hdfs-to-bq.etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq.order_details` AS od
                    ),
                    orders AS (
                        SELECT
                            o.ORDERID,
                            o.SHIPPEDDATE
                        FROM `etl-use-airflow-fr-hdfs-to-bq.etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq.orders` AS o
                    ),
                    products AS (
                        SELECT
                            p.PRODUCTID,
                            p.SUPPLIERID
                        FROM `etl-use-airflow-fr-hdfs-to-bq.etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq.products` AS p
                    )
                    SELECT
                        s.COMPANYNAME AS supplier_name,
                        FORMAT_DATE('%Y-%m', o.SHIPPEDDATE) AS month,
                        SUM(od.gross_revenue) AS gross_revenue
                    FROM order_details od
                    JOIN orders o ON od.ORDERID = o.ORDERID
                    JOIN `etl-use-airflow-fr-hdfs-to-bq.etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq.suppliers` AS s ON p.SUPPLIERID = s.SUPPLIERID
                    GROUP BY s.COMPANYNAME, FORMAT_DATE('%Y-%m', o.SHIPPEDDATE)
                    ORDER BY month DESC
                ''',
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": 'etl-use-airflow-fr-hdfs-to-bq',
                    "datasetId": 'etl-use-airflow-fr-hdfs-to-bq.airflow_postgres_to_bq',
                    "tableId": 'dm_monthly_supplier_gross_revenue'
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    create_dataset >> create_dm_monthly_supplier_gross_revenue
