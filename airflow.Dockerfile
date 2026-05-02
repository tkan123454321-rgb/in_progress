FROM apache/airflow:3.2.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git 
USER airflow
COPY pyproject.toml .
RUN pip install --no-cache-dir ".[ingest]"
ADD requirements_dbt.txt .
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir ".[dbt]"