FROM apache/airflow:2.9.0-python3.8

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0 psycopg2-binary==2.9.10 importlib-metadata

ENV SPARK_HOME=/home/airflow/.local/lib/python3.8/site-packages/pyspark
ENV PATH="$SPARK_HOME/bin:$PATH"
