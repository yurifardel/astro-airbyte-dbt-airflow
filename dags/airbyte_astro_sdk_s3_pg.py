from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata
from airflow.datasets import Dataset
from datetime import datetime

airbyte_source_id = '<source_id>'
airbyte_connection_id = '<connection_id>'
airbyte_conn = 'airbyte_connect'
aws_conn = 'aws_connect'
postgres_conn = 'pg_connect'
cliente = "<cliente>"
start_date = "2024-10-22T00:00:00Z"
end_date = "2024-10-23T00:00:00Z"
landing_zone_path = "s3://dvlp-airbyte-ingestion/"

propostas_parquet_dataset = Dataset("s3://dvlp-airbyte-ingestion/bronze/")

def token_generator_api():
    base_connection_api = BaseHook.get_connection('api')
    senha = Variable.get('api_secret')
    url = base_connection_api.host
    login = base_connection_api.login

    credentials = {
        "login": login,
        "senha": senha
    }

    header = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'Referer': f''
    }

    response = requests.post(f'{url}/auth/tokens', json=credentials, headers=header)

    token = response.json()

    if 'data' in token:
        return token['data']
    

def token_generator_airbyte():
    airbyte_base_connection = BaseHook.get_connection('airbyte_connect')
    url = airbyte_base_connection.host
    client_id = airbyte_base_connection.login
    secret_id = airbyte_base_connection.password

    print(url)

    payload = {
        "grant-type": "client_credentials",
        "client_id": client_id,
        "client_secret": secret_id
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(f'{url}/token', json=payload, headers=headers)

    token = response.json()

    if 'access_token' in token:
        return token['access_token']

    

def sync_airbyte_params():
    airbyte_base_connection = BaseHook.get_connection('airbyte_connect')
    base_connection_api = BaseHook.get_connection('api')

    api_senha = Variable.get('api_secret')
    api_login = base_connection_api.login
    airbyte_host = airbyte_base_connection.host

    airbyte_token = token_generator_airbyte()
    api_token = token_generator_api()


    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f'Bearer {airbyte_token}'
    }

    payload = { "configuration": { 
            "cliente": cliente,
            "start_date": start_date,
            "end_date": end_date,
            "login": api_login,
            "senha": api_senha,
            "token_api": api_token
        } 
    }

    response = requests.patch(f'{airbyte_host}/sources/{airbyte_source_id}', headers=headers, json=payload)

    print(response.json())



def create_dag() -> None:
    dag = DAG(
        dag_id='dvlp-airbyte-astro',
        start_date=datetime(2024, 10, 1),
        schedule_interval=None
    )
    with dag:
        start = DummyOperator(
            task_id='start'
        )

        airbyte_sync_params = PythonOperator(
            task_id='sync_airbyte_params',
            python_callable=sync_airbyte_params
        )

        trigger_airbyte_sync_api_s3 = AirbyteTriggerSyncOperator(
            task_id='trigger_airbyte_sync',
            connection_id=airbyte_connection_id,
            airbyte_conn_id=airbyte_conn,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        )

        proposta_parquet = aql.load_file(
            task_id="proposta_parquet",
            input_file=File(path=landing_zone_path + "bronze/propostas", filetype=FileType.PARQUET, conn_id=aws_conn),
            output_table=Table(name="proposta", metadata=Metadata(schema="silver"), conn_id=postgres_conn),
            if_exists="replace",
            use_native_support=True,
            outlets=[propostas_parquet_dataset]
        )    

        end = DummyOperator(
            task_id='end'
        )

    start >> airbyte_sync_params >> trigger_airbyte_sync_api_s3 >> proposta_parquet >> end

create_dag()