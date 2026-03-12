from airflow import DAG
from airflow.operators.bash import BashOperator
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

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="docker exec spark-streaming python3 /app/streaming/spark_stream_orders.py"
    )


# Airflow
#    │
#    ▼
# docker exec
#    │
#    ▼
# spark-streaming container
#    │
#    ▼
# spark_stream_orders.py