from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime

dag = DAG(
    dag_id='insert_into_bigquery',
    start_date=datetime(2024, 1, 20),
    schedule_interval=None,
    catchup=False
)


def insert_data(**kwargs):
    fetched_data = Variable.get("FETCHED_DATA")

    print(fetched_data)


insert_into_bigquery_task = PythonOperator(
    task_id='insert_into_bigquery_task',
    python_callable=insert_data,
    provide_context=True,  # This is needed to access the task instance in the function
    dag=dag
)
