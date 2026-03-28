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
from schema.producer_schema import KafkaMetadataHistoricalQuotes, KafkaMetadataFundamental, KafkaMetadataFinancialReports, KafkaMetadataBalanceSheet, KafkaMetadataCashFlowDirect, KafkaMetadataIncomeStatement, KafkaMetadataCashFlowIndirect
from ingestion.generate_data_metadata import _generate_metadata_fundamental, _generate_metadata_historical, _generate_metadata_financial_reports
financiel_model_list =[KafkaMetadataBalanceSheet, KafkaMetadataCashFlowDirect, KafkaMetadataIncomeStatement, KafkaMetadataCashFlowIndirect]
for model_cls in financiel_model_list:
    ingest_main(
            model_cls=model_cls,
            generate_metadata_callable=_generate_metadata_financial_reports,
            ticker_list_mode="other_data"
        )

# %%
from utils.other_utils import _get_session
import polars as pl

with _get_session() as s:
    response = s.get("https://restv2.fireant.vn/symbols/AAA")
    print(response.json())
    


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

# %%
