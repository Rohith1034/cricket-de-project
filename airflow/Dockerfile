FROM apache/airflow:3.2.1

USER root

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    google-cloud-storage \
    apache-airflow-providers-google \
    numpy \
    pandas