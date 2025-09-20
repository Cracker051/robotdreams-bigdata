from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG(dag_id="etl-pipeline", start_date=datetime(2023, 1, 1), catchup=False, tags=["etl"]) as dag:
    spark_job = SparkSubmitOperator(
        task_id="spark_extract_load",
        application="/opt/airflow/dags/operators/spark_extract.py",
        conn_id="spark_conn",
        jars="/opt/spark/artifacts/postgresql-42.7.7.jar",
    )

    dbt_models_job = BashOperator(
        task_id="dbt_models",
        bash_command="dbt run --project-dir /opt/dbt/nyc_taxi_dbt --profiles-dir /opt/dbt/nyc_taxi_dbt",
    )

    dbt_tests_job = BashOperator(
        task_id="dbt_tests",
        bash_command="dbt test --project-dir /opt/dbt/nyc_taxi_dbt --profiles-dir /opt/dbt/nyc_taxi_dbt",
    )

    spark_job >> dbt_models_job >> dbt_tests_job
