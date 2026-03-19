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
from utils.minio_maintenance import LakehouseMaintenance
from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient

maintenance_instance = LakehouseMaintenance(lake_client=LakeHouseClient(), pg_client=PostgresClient(PostgresClient.get_db_connection("platform_db")))
maintenance_instance.run_full_iceberg_maintenance()
# %%
from utils.minio_maintenance import minio_maintenance
minio_maintenance(db_name="platform_db")


# %%

    



# %%

# %%
