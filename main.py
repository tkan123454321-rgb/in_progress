# %%
import math
import os
import typing
from urllib import request

from dotenv import load_dotenv
from pathlib import Path
import re
import yaml # type: ignore
from ingestion.generate_data_metadata import KafkaMetadataHistoricalQuotes, _generate_metadata_financial_reports, _generate_metadata_fundamental
from schema.producer_schema import KafkaMetadataCashFlowIndirect, KafkaMetadataFundamental, KafkaMetadataIncomeStatement
from utils.logger_config import setup_logger

# %%
from ingestion.ingest_main import ingest_main
from load.load_main import load_main
from schema.producer_schema import FinancialReportsYearBalanceSheet, FinancialReportsYearIncomeStatement, FinancialReportsYearCashFlowDirect, FinancialReportsYearCashFlowIndirect, FinancialReportsYear, BaseMetadata, HistoricalQuotes, FinancialReportsQuarterCashFlowDirect, FinancialReportsQuarterCashFlowIndirect, FinancialReportsQuarterIncomeStatement, FinancialReportsQuarterBalanceSheet
financial_report_list = [
    FinancialReportsQuarterBalanceSheet,
    FinancialReportsQuarterIncomeStatement,
    FinancialReportsQuarterCashFlowDirect,
    FinancialReportsQuarterCashFlowIndirect,
]
for model_cls in financial_report_list:
    load_main(
        model_cls=model_cls,
    )


# %%
from utils.minio_maintenance import LakehouseMaintenance
from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
with LakehouseMaintenance(lake_client=LakeHouseClient(), pg_client=PostgresClient()) as maintenance:
    maintenance.run_full_iceberg_maintenance()



# %%
from load.load_main import load_main
from schema.producer_schema import Fundamental_1, Fundamental_2
fundamental_list = [Fundamental_1, Fundamental_2]
for model_cls in fundamental_list:
    load_main(
        model_cls=model_cls,
    )


# %%
from schema.producer_schema import Fundamental_1, Fundamental_2
from ingestion.ingest_main import ingest_main
from ingestion.generate_data_metadata import _generate_metadata_fundamental
fundamental_list = [Fundamental_1, Fundamental_2]
for model_cls in fundamental_list:
    ingest_main(
        model_cls=model_cls,
        generate_metadata_callable=_generate_metadata_fundamental
    )

# %%
from load.lakehouse_loader import LakehouseLoader
from schema.producer_schema import OriginalTickerList
loader = LakehouseLoader()
loader._put_original_ticker_list(OriginalTickerList)
# %%
from utils.metadata_manager import MetadataManager
from utils.postgres_client import PostgresClient
from utils.lakehouse_client import LakeHouseClient
pg_client = PostgresClient()
lake_client = LakeHouseClient()
with MetadataManager(pg_client=pg_client, lake_client=lake_client) as m:
    with m.trino_conn.cursor() as cur:
        cur.execute("SELECT 1, 2")
        print(type(cur.fetchall()))
# %%
from load.lakehouse_loader import LakehouseLoader
from schema.producer_schema import OriginalTickerList
loader = LakehouseLoader()
loader._put_original_ticker_list(OriginalTickerList)
# %%
