from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import ast

dag = DAG(
    dag_id='insert_into_bigquery',
    start_date=datetime(2024, 1, 20),
    schedule_interval=None,
    catchup=False
)


def create_table_if_not_exists():
    BIGQUERY_CONN_ID = Variable.get("BIGQUERY_CONN_ID")
    PROJECT_ID = Variable.get("PROJECT_ID")
    DATASET_ID = Variable.get("DATASET_ID")
    TABLE_ID = Variable.get("TABLE_ID")

    bigquery_conn_id = BIGQUERY_CONN_ID
    hook = BigQueryHook(bigquery_conn_id)

    schema = [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "latitude", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "longitude", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "altitude", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "velocity", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "visibility", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "footprint", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "units", "type": "STRING", "mode": "REQUIRED", "description": "STRING(25)"},
    ]

    hook.create_empty_table(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        schema_fields=schema,
    )


create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table_if_not_exists,
    provide_context=True,
    dag=dag
)

def insert_data():
    BIGQUERY_CONN_ID = Variable.get("BIGQUERY_CONN_ID")
    PROJECT_ID = Variable.get("PROJECT_ID")
    DATASET_ID = Variable.get("DATASET_ID")
    TABLE_ID = Variable.get("TABLE_ID")
    FETCHED_DATA = Variable.get("FETCHED_DATA")

    bigquery_conn_id = BIGQUERY_CONN_ID
    hook = BigQueryHook(bigquery_conn_id)

    data_dict = ast.literal_eval(FETCHED_DATA)  # had to use this because it looked like a dict but was a str

    hook.insert_all(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        rows=[data_dict]  # Wrap the single dictionary in a list
    )


insert_into_bigquery_task = PythonOperator(
    task_id='insert_into_bigquery_task',
    python_callable=insert_data,
    provide_context=True,  # This is needed to access the task instance in the function
    dag=dag
)

create_table_task >> insert_into_bigquery_task