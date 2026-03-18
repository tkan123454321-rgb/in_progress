# %%
import os
import typing
from urllib import request

from dotenv import load_dotenv
from pathlib import Path
import re
import yaml # type: ignore
from schema.producer_schema import KafkaMetadataFundamental
from utils.logger_config import setup_logger

# %%
from utils.postgres_client import PostgresClient
with PostgresClient.get_db_connection(db_name="platform_db") as conn:
    pg_client = PostgresClient(conn=conn)
    pg_client.prepare_maintenance_queue()
# %%
from utils.minio_maintenance import minio_maintenance
minio_maintenance(db_name="platform_db")


# %%

    



# %%

# %%
