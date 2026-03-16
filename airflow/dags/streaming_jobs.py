from __future__ import annotations

import pendulum  # type: ignore[import-untyped]
from airflow import DAG  # type: ignore[import-untyped]
from airflow.operators.bash import BashOperator  # type: ignore[import-untyped]

SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.postgresql:postgresql:42.7.3",
    ]
)
SPARK_IVY_DIR = "/opt/airflow/.ivy2/shared"
SPARK_DRIVER_PYTHONPATH = "/opt/airflow/src"
SPARK_EXECUTOR_PYTHONPATH = "/opt/spark-apps"
SPARK_DRIVER_PYTHON = "/home/airflow/.local/bin/python3"
SPARK_EXECUTOR_PYTHON = "/usr/bin/python3"
SPARK_SUBMIT = (
    f'PYTHONPATH="{SPARK_DRIVER_PYTHONPATH}" '
    f'PYSPARK_DRIVER_PYTHON="{SPARK_DRIVER_PYTHON}" '
    f'PYSPARK_PYTHON="{SPARK_EXECUTOR_PYTHON}" '
    "spark-submit --master spark://spark-master:7077 "
    f'--packages "{SPARK_PACKAGES}" '
    f'--conf spark.jars.ivy="{SPARK_IVY_DIR}" '
    f'--conf spark.driverEnv.PYTHONPATH="{SPARK_DRIVER_PYTHONPATH}" '
    f'--conf spark.executorEnv.PYTHONPATH="{SPARK_EXECUTOR_PYTHONPATH}" '
    f'--conf spark.pyspark.driver.python="{SPARK_DRIVER_PYTHON}" '
    f'--conf spark.pyspark.python="{SPARK_EXECUTOR_PYTHON}" '
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)
SPARK_RESOURCES = (
    "--conf spark.cores.max=2 "
    "--conf spark.executor.cores=2 "
    "--conf spark.sql.shuffle.partitions=2 "
    "--conf spark.executor.memory=1024m "
    "--conf spark.executor.memoryOverhead=256m "
    "--conf spark.driver.memory=768m"
)
JOB1 = "/opt/airflow/src/de_challenge/spark/job1.py"
JOB2 = "/opt/airflow/src/de_challenge/spark/job2.py"
DAG_TIMEZONE = pendulum.timezone("America/Recife")

with DAG(
    dag_id="trips_streaming_pipeline",
    description="Run trips streaming pipeline jobs",
    start_date=pendulum.datetime(2024, 1, 1, tz=DAG_TIMEZONE),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    job1 = BashOperator(
        task_id="bronze_to_silver_stream",
        bash_command=f"{SPARK_SUBMIT} {SPARK_RESOURCES} {JOB1}",
        retries=2,
        retry_delay=pendulum.duration(seconds=60),
    )

    job2 = BashOperator(
        task_id="silver_to_gold_stream",
        bash_command=f"{SPARK_SUBMIT} {SPARK_RESOURCES} {JOB2}",
        retries=2,
        retry_delay=pendulum.duration(seconds=30),
    )
