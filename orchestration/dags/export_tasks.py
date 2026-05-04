from airflow.sdk import task
from load.web_serving_loader import WebServingLoader
from common.clients.lakehouse_client import LakeHouseClient
from common.clients.postgres_client import PostgresClient


@task(task_display_name="csv frontend serving task")
def csv_frontend_serving_task():
    """
    This task is defined as a separated task to be callable from multiple DAGs in `main_dags.py`. It's main purpose is to adhere to the DRY principle and keep main dags clean.
    Exports the finalized OBT table directly from the Iceberg catalog to a local CSV file for static frontend serving.
    """
    loader = WebServingLoader(pg_client=PostgresClient(), lake_client=LakeHouseClient())
    loader.export_web_data()
