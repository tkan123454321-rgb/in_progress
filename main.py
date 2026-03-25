# %%
import os
import typing
from urllib import request

from dotenv import load_dotenv
from pathlib import Path
import re
import yaml # type: ignore
from ingestion.generate_data_metadata import KafkaMetadataHistoricalQuotes, _generate_metadata_fundamental
from schema.producer_schema import KafkaMetadataFundamental
from utils.logger_config import setup_logger

# %%
from ingestion.ingest_main import ingest_main
from schema.producer_schema import KafkaMetadataHistoricalQuotes, KafkaMetadataFundamental
from ingestion.generate_data_metadata import _generate_metadata_fundamental, _generate_metadata_historical
ingest_main(
    model_cls=KafkaMetadataHistoricalQuotes,
    generate_metadata_callable=_generate_metadata_historical,
    ticker_list_mode="other_data"
)

# %%
from utils.minio_maintenance import LakehouseMaintenance
from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
from utils.metadata_manager import MetadataManager
with MetadataManager(pg_client = PostgresClient(), lake_client=LakeHouseClient()) as metadata_manager:
    metadata_manager._update_max_ingested_date()


# %%
from utils.other_utils import _get_session
from datetime import date
import time
session = _get_session()
# response = session.get("https://restv2.fireant.vn/symbols/VNM/historical-quotes?startDate=2020-03-02&endDate=2026-03-19&offset=0&limit=50")
# response.json()
start_date = date(2022, 3, 15)
end_date = date(2026, 3, 19)
day_diffs = (end_date - start_date).days
estimate_trading_days = int(day_diffs * (250 / 365)) + 20
print (f"Estimated trading days between {start_date} and {end_date}: {estimate_trading_days}")
for offset in range(0, estimate_trading_days, 500):
    url = f"https://restv2.fireant.vn/symbols/VNM/historical-quotes?startDate={start_date}&endDate={end_date}&offset={offset}&limit=500"
    print(url)
    response = session.get(url)
    print(response.json())
    print(f"{len(response.json())} records retrieved for offset {offset}")
    time.sleep(1)  # Sleep for 1 second to avoid hitting rate limits





# %%

# %%
