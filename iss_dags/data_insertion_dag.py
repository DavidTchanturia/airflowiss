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
