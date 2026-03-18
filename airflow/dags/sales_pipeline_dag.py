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

    run_analytics_model = BigQueryInsertJobOperator(
        task_id="run_sales_analytics",
        project_id="meli-data-platform",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `meli-data-platform.meli_analytics.sales_by_category_hourly` AS
                SELECT
                    category,
                    SUM(total_value) AS total_sales
                FROM `meli-data-platform.meli_raw.orders_streams`
                WHERE DATE(event_time) = CURRENT_DATE()
                GROUP BY category
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )