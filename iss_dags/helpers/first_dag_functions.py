from airflow.models.variable import Variable
from datetime import datetime
import requests


def get_credentials() -> dict:
    """all the necessary variables have been defined in airflow UI
    function retrieves credentials from the airflow variables. returns them as dict"""

    BIGQUERY_CONN_ID = Variable.get("BIGQUERY_CONN_ID")
    PROJECT_ID = Variable.get("PROJECT_ID")
    DATASET_ID = Variable.get("DATASET_ID")
    TABLE_ID = Variable.get("TABLE_ID")
    FETCHED_DATA = Variable.get("FETCHED_DATA")

    credentials = {"BIGQUERY_CONN_ID": BIGQUERY_CONN_ID, "PROJECT_ID": PROJECT_ID,
                   "DATASET_ID": DATASET_ID, "TABLE_ID": TABLE_ID, "FETCHED_DATA": FETCHED_DATA}

    return credentials


def fetch_data(**kwargs) -> None:
    """fetches data from the url present in variables on bigquery

    pushes the response to xcom as a json
    """

    api_url = Variable.get("API_URL")

    if not api_url:
        raise ValueError("url does not exist")

    response = requests.get(api_url)
    data = response.json()

    kwargs['ti'].xcom_push(key='fetched_data', value=data)


def transform_data(**kwargs) -> None:
    """
    retrieves pushed data to xcom by fetch_data function

    besides basic transformation, adds id field to it, which is a variable on airflow as well,
    each time id is incremented by one

    instead of pushing transformed data to xcom, pushes it to variable. Since this data needs to be
    accessed by another dag
    """

    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids='fetch_iss_data_task', key='fetched_data')
    id = int(Variable.get("id"))

    fetched_data.pop('id')
    fetched_data.pop('daynum')
    fetched_data.pop('solar_lat')
    fetched_data.pop('solar_lon')

    fetched_data['velocity'] = round(float(fetched_data['velocity']), 5)
    fetched_data['units'] = "km"
    fetched_data['timestamp'] = datetime.utcfromtimestamp(fetched_data['timestamp']).strftime("%Y-%m-%d %H:%M:%S")
    fetched_data['visibility'] = True if fetched_data['visibility'] == "daylight" else False
    fetched_data.update({"id": id})

    Variable.set("id", id + 1)
    kwargs['ti'].xcom_push(key='FETCHED_DATA', value=fetched_data)
