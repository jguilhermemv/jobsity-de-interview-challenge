FROM apache/airflow:2.9.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.2.0

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="$SPARK_HOME/bin:$PATH"

USER airflow
