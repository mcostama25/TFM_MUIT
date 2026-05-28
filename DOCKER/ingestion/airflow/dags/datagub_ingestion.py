from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "datahub_periodic_ingestion",
    default_args={"owner": "airflow"},
    description="DAG para la ingesta periódica de DataHub",
    start_date=datetime(2026, 1, 1),
    schedule_interval=timedelta(minutes=30),
    catchup=False,
) as dag:
    ingest_task = BashOperator(
        task_id="ingest_from_datahub",
        bash_command="datahub ingest run -c /opt/airflow/recipes/recipe.yml",
    )
