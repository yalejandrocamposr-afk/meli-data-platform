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

# Cada que corre genera en BQ una analtica de categorias vendidas del día. 
    run_analytics_model = BashOperator(
        task_id="run_sales_analytics",
        bash_command="""
        bq query --use_legacy_sql=false '
        CREATE OR REPLACE TABLE `meli-data-platform.meli_analytics.sales_by_category_hourly` AS
        SELECT
            category,
            SUM(total_value) as total_sales
        FROM `meli-data-platform.meli_raw.orders`
        WHERE DATE(event_time) = CURRENT_DATE()
        GROUP BY category
        '
        """
    )