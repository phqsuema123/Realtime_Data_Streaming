FROM apache/airflow:2.6.0-python3.9

USER airflow

RUN pip install --no-cache-dir \
    kafka-python \
    pandas \
    requests \
    cassandra-driver