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
from schema.producer_schema import FinancialReportsYearBalanceSheet, FinancialReportsYearIncomeStatement, FinancialReportsYearCashFlowDirect, FinancialReportsYearCashFlowIndirect, FinancialReportsYear, BaseMetadata, HistoricalQuotes, FinancialReportsQuarterCashFlowDirect, FinancialReportsQuarterCashFlowIndirect, FinancialReportsQuarterIncomeStatement, FinancialReportsQuarterBalanceSheet, FundamentalQuarter, VNINDEXHistoricalQuotes
financial_report_list = [
    FinancialReportsQuarterBalanceSheet,
    FinancialReportsQuarterIncomeStatement,
    FinancialReportsQuarterCashFlowDirect,
    FinancialReportsQuarterCashFlowIndirect,
]
load_main(
        model_cls=VNINDEXHistoricalQuotes,
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
from utils.other_utils import _get_session
with _get_session() as session:
    response = session.get("https://www.worldgovernmentbonds.com/wp-json/common/v1/historical")
    print(response.json())
# %%
import polars as pl
from datetime import datetime
from zoneinfo import ZoneInfo
message = [
    {
	"data_type": "fundamental_quarter",
	"source": "fireant",
	"created_at_ts": "2026-04-05T03:20:44.540393Z",
	"batch_id": "554d077f-d931-4454-b334-b800698fb882",
	"ticker": "HT1",
	"url": "https://restv2.fireant.vn/symbols/HT1/financial-data?type=Q&count=34",
    "value": [
    {
        "symbol": "VIX",
        "year": 2025,
        "quarter": 4,
        "companyType": "Securities",
        "icbCode": "30202005",
        "icbName": "Công ty chứng khoán",
        "financialValues": {
            "Quarter": 4,
            "Year": 2025,
            "CompanyType": "Securities",
            "CurrentAsset": 34112186395621,
            "ShorttermFinancialAsset": 34109710475495,
            "CashAndCashEquivalent": 2049092199336,
            "Cash": 1179092199336,
        }
    },
    {
        "symbol": "VIX",
        "year": 2025,
        "quarter": 3,
        "companyType": "Securities",
        "icbCode": "30202005",
        "icbName": "Công ty chứng khoán",
        "financialValues": {
            "Quarter": 3,
            "Year": 2025,
            "CompanyType": "Securities",
            "CurrentAsset": 31490793258294,
            "ShorttermFinancialAsset": 31486987908373,
            "CashAndCashEquivalent": 909902606614,
            "Cash": 183902606614,
        }
    }]
 },
{
	"data_type": "fundamental_quarter",
	"source": "fireant",
	"created_at_ts": "2026-04-05T03:20:44.540393Z",
	"batch_id": "554d077f-d931-4454-b334-b800698fb882",
	"ticker": "VIC",
	"url": "https://restv2.fireant.vn/symbols/VIC/financial-data?type=Q&count=34",
    "value": [
    {
        "symbol": "VIC",
        "year": 2025,
        "quarter": 4,
        "companyType": "Securities",
        "icbCode": "30202005",
        "icbName": "Công ty chứng khoán",
        "financialValues": {
            "Quarter": 4,
            "Year": 2025,
            "CompanyType": "Securities",
            "CurrentAsset": 34112186395621,
            "ShorttermFinancialAsset": 34109710475495,
            "CashAndCashEquivalent": 2049092199336,
            "Cash": 1179092199336,
        }
    },
    {
        "symbol": "VIC",
        "year": 2025,
        "quarter": 3,
        "companyType": "Securities",
        "icbCode": "30202005",
        "icbName": "Công ty chứng khoán",
        "financialValues": {
            "Quarter": 3,
            "Year": 2025,
            "CompanyType": "Securities",
            "CurrentAsset": 31490793258294,
            "ShorttermFinancialAsset": 31486987908373,
            "CashAndCashEquivalent": 909902606614,
            "Cash": 183902606614,
        }
    }]
 },

]

        
lf = pl.LazyFrame(message)
df = lf.explode("value").unnest("value")
df = df.with_columns(
    pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time"),
    pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC")
)
df = df.collect()
df.glimpse()



# %%
