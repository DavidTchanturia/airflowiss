from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from helpers.contants import schema
from helpers.first_dag_functions import get_credentials

import ast


def create_table_if_not_exists():
    credentials = get_credentials()

    hook = BigQueryHook(credentials["BIGQUERY_CONN_ID"])

    hook.create_empty_table(
        project_id=credentials["PROJECT_ID"],
        dataset_id=credentials["DATASET_ID"],
        table_id=credentials["TABLE_ID"],
        schema_fields=schema,
    )


def insert_data():
    credentials = get_credentials()

    hook = BigQueryHook(credentials["BIGQUERY_CONN_ID"])

    data_dict = ast.literal_eval(credentials["FETCHED_DATA"])  # had to use this because it looked like a dict but was a str

    hook.insert_all(
        project_id=credentials["PROJECT_ID"],
        dataset_id=credentials["DATASET_ID"],
        table_id=credentials["TABLE_ID"],
        rows=[data_dict]
    )