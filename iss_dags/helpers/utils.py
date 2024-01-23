from airflow.models.variable import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import ast
import requests


def get_credentials():
    BIGQUERY_CONN_ID = Variable.get("BIGQUERY_CONN_ID")
    PROJECT_ID = Variable.get("PROJECT_ID")
    DATASET_ID = Variable.get("DATASET_ID")
    TABLE_ID = Variable.get("TABLE_ID")
    FETCHED_DATA = Variable.get("FETCHED_DATA")

    credentials = {"BIGQUERY_CONN_ID": BIGQUERY_CONN_ID, "PROJECT_ID": PROJECT_ID,
                   "DATASET_ID": DATASET_ID, "TABLE_ID": TABLE_ID, "FETCHED_DATA": FETCHED_DATA}

    return credentials


#################################### these are for the first dag ########################################
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
    id = int(Variable.get("id"))

    del fetched_data['id']
    del fetched_data['daynum']
    del fetched_data['solar_lat']
    del fetched_data['solar_lon']

    fetched_data['velocity'] = round(float(fetched_data['velocity']), 5)
    fetched_data['units'] = "km"
    fetched_data['timestamp'] = datetime.utcfromtimestamp(fetched_data['timestamp']).strftime("%Y-%m-%d %H:%M:%S")
    fetched_data['visibility'] = True if fetched_data['visibility'] == "daylight" else False
    fetched_data.update({"id": id})

    Variable.set("FETCHED_DATA", fetched_data)
    Variable.set("id", id + 1)


#################################### these are for the second dag ########################################
def create_table_if_not_exists():
    credentials = get_credentials()

    hook = BigQueryHook(credentials["BIGQUERY_CONN_ID"])
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
