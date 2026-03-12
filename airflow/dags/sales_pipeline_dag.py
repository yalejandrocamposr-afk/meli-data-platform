from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    "meli_sales_pipeline",
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args
) as dag:

# Cada hora que corre genera en BQ una analtica de categorias vendidas. 
    run_sales_analytics = BashOperator(
        task_id="run_sales_analytics",
        bash_command="bq query --use_legacy_sql=false < /opt/airflow/models/sales_by_category_hourly.sql"
    )