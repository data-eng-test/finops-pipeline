# Airflow DAG — runs daily at 06:00 UTC
# Ingests AWS + GCP billing, transforms via dbt, loads to BigQuery

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "sarah.chen",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["sarah@company.com"],
}

with DAG(
    "finops_daily",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finops", "billing", "daily"],
) as dag:

    wait_for_aws = TimeDeltaSensor(
        task_id="wait_for_aws_export",
        delta=timedelta(hours=1),
    )

    ingest_aws = BashOperator(
        task_id="ingest_aws",
        bash_command="python ingest/aws.py --date={{ ds }}",
    )

    ingest_gcp = BashOperator(
        task_id="ingest_gcp",
        bash_command="python ingest/gcp.py --date={{ ds }}",
    )

    normalize = BashOperator(
        task_id="normalize_tags",
        bash_command="python transform/normalize.py --date={{ ds }}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select finops --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --select finops --target prod",
    )

    load_bq = BashOperator(
        task_id="load_bq",
        bash_command="python load/bigquery.py --date={{ ds }}",
    )

    notify_slack = BashOperator(
        task_id="notify_slack",
        bash_command="python load/notify.py --status=success --date={{ ds }}",
    )

    wait_for_aws >> ingest_aws
    [ingest_aws, ingest_gcp] >> normalize >> dbt_run >> dbt_test >> load_bq >> notify_slack
