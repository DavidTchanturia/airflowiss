from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from helpers.utils import create_table_if_not_exists, insert_data

dag = DAG(
    dag_id='insert_into_bigquery',
    start_date=datetime(2024, 1, 20),
    schedule_interval=None,
    catchup=False
)

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table_if_not_exists,
    provide_context=True,
    dag=dag
)


insert_into_bigquery_task = PythonOperator(
    task_id='insert_into_bigquery_task',
    python_callable=insert_data,
    provide_context=True,
    dag=dag
)

create_table_task >> insert_into_bigquery_task

