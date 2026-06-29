# Airflow worker image with Java (for PySpark) baked in.
# Source is files only (8 CSVs), so no MySQL/JDBC drivers are needed here.
# dbt + the SQL Server loader run on the WINDOWS HOST (Windows auth), not in this image.
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
# pyarrow lets the bronze extractor write Parquet without pandas' optional deps surprises.
RUN pip install --no-cache-dir \
      pyspark==3.5.1 \
      pyarrow==16.1.0 \
      pandas==2.2.2
