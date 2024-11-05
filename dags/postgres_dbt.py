import os
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from datetime import datetime


propostas_parquet_dataset = Dataset("s3://dvlp-airbyte-ingestion/bronze/")


default_dbt_root_path = Path(__file__).parent / "dbt"
dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))

postgres_conn = 'pg_connect'
connection = BaseHook.get_connection(postgres_conn)

profile_config = ProfileConfig(
    profile_name="silver-charmer",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=postgres_conn,
        profile_args={
            "schema": "gold",
            "raw_schema": "silver"
        }
    ),
)

def dbt_analytics_project() -> None:
    dag = DAG(
        dag_id="dvlp-analytics-dbt",
        start_date=datetime(2024, 10, 1),
        max_active_runs=1,
        schedule=[propostas_parquet_dataset]
    )
    with dag:
        start = EmptyOperator(task_id="start")

        analytics = DbtTaskGroup(
            project_config=ProjectConfig(
                (dbt_root_path / "analytics").as_posix()
            ),
            profile_config=profile_config,
            operator_args={
                "install_deps": True
            }
        )

        end = EmptyOperator(task_id="end")

    start >> analytics >> end


dbt_analytics_project()