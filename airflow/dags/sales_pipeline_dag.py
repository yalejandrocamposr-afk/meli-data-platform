from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator # type: ignore
from datetime import datetime

with open("/opt/airflow/models/sales_by_category_hourly.sql") as f:
    SALES_QUERY = f.read()

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 2
}

with DAG(
    dag_id="meli_sales_pipeline",
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["meli","analytics"]
) as dag:
    
    # Cada hora que corre genera en BQ una analtica de categorias vendidas. 
    run_sales_analytics = BigQueryInsertJobOperator( 
        task_id="run_sales_analytics",
        project_id="meli-data-platform",
        configuration={
            "query": {
                "query": SALES_QUERY,
                "useLegacySql": False
            }
        },
        location="US"
    )