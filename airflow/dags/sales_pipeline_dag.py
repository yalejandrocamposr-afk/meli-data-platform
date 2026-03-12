from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator # type: ignore
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
    with open("/opt/airflow/models/sales_by_category_hourly.sql") as f:
        sales_query = f.read()

    run_sales_analytics = BigQueryInsertJobOperator(
        task_id="run_sales_analytics",
         project_id="meli-data-platform",
        configuration={
            "query": {
                "query": sales_query,
                "useLegacySql": False,
            }
        },
        location="US"
    )