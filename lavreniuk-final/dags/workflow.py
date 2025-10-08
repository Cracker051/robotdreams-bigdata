from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(dag_id="workflow", start_date=datetime(2023, 1, 1), catchup=False, tags=["etl"]) as dag:
    bronze_layer_job = SparkSubmitOperator(
        task_id="spark_bronze_layer",
        application="/opt/spark/jobs/bronze.py",
        conn_id="spark_conn",
        jars="/opt/spark/artifacts/postgresql-42.7.7.jar",
    )

    silver_layer_job = SparkSubmitOperator(
        task_id="spark_silver_layer",
        application="/opt/spark/jobs/silver.py",
        conn_id="spark_conn",
        jars="/opt/spark/artifacts/postgresql-42.7.7.jar",
    )

    gold_layer_job = BashOperator(
        task_id="dbt_gold_layer",
        bash_command="dbt run --project-dir /opt/dbt/dbt_transform --profiles-dir /opt/dbt/dbt_transform",
    )

    dbt_tests_job_job = BashOperator(
        task_id="dbt_tests",
        bash_command="dbt test --project-dir /opt/dbt/dbt_transform --profiles-dir /opt/dbt/dbt_transform",
    )

    bronze_layer_job >> silver_layer_job >> gold_layer_job >> dbt_tests_job_job
