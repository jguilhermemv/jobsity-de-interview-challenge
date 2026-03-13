from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = "spark-submit"
JOB1 = "/opt/airflow/src/de_challenge/spark/job1.py"
JOB2 = "/opt/airflow/src/de_challenge/spark/job2.py"

with DAG(
    dag_id="streaming_jobs",
    description="Run Spark Structured Streaming jobs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    job1 = BashOperator(
        task_id="spark_job_1",
        bash_command=f"{SPARK_SUBMIT} {JOB1}",
        retries=1,
    )

    job2 = BashOperator(
        task_id="spark_job_2",
        bash_command=f"{SPARK_SUBMIT} {JOB2}",
        retries=1,
    )

    job1 >> job2
