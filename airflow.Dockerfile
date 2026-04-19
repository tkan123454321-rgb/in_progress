FROM apache/airflow:3.2.0
ADD requirements_ingest.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements_ingest.txt
ADD requirements_dbt.txt .
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r requirements_dbt.txt