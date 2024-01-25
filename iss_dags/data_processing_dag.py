from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from helpers.first_dag_functions import fetch_data, transform_data

dag = DAG(
    "iss_data_processing",
    start_date=datetime(2024, 1, 21),
    schedule_interval=timedelta(hours=1),
    catchup=False
)

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

trigger_insert_into_bigquery_dag = TriggerDagRunOperator(
    task_id="trigger_insert_into_bigquery_dag",
    trigger_dag_id="insert_into_bigquery",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

fetch_iss_data_task >> transform_iss_data_task >> trigger_insert_into_bigquery_dag
