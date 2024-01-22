from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests


dag = DAG(
    "iss_data_processing",
    start_date=datetime(2024, 1, 21),
    schedule_interval=timedelta(hours=1)
)


def fetch_data(**kwargs):
    api_url = Variable.get("API_URL")

    if not api_url:
        raise ValueError("url does not exist")

    response = requests.get(api_url)
    data = response.json()

    kwargs['ti'].xcom_push(key='fetched_data', value=data)


def transform_data(**kwargs):
    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids='fetch_iss_data_task', key='fetched_data')

    del fetched_data['id']
    fetched_data['velocity'] = round(float(fetched_data['velocity']), 5)
    fetched_data['units'] = "km"
    fetched_data['timestamp'] = datetime.utcfromtimestamp(fetched_data['timestamp']).strftime("%Y-%m-%d %H:%M:%S")

    kwargs['ti'].xcom_push(key='fetched_data', value=fetched_data)


fetch_iss_data_task = PythonOperator(
    task_id="fetch_iss_data_task",
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

transform_iss_data_task = PythonOperator(
    task_id="transform_iss_data_task",
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

fetch_iss_data_task >> transform_iss_data_task
