# %%
import test
class macro:
    arg = None
    @classmethod
    def __getattr__(cls, name):
        arg = test
        return getattr(arg, name)
    
test_thu = macro.arg
print(test_thu)

# %%
from ingestion.ingest_main import ingest_main
from load.load_main import load_main
from schema.producer_schema import FinancialReportsYearBalanceSheet, FinancialReportsYearIncomeStatement, FinancialReportsYearCashFlowDirect, FinancialReportsYearCashFlowIndirect, FinancialReportsYear, BaseMetadata, HistoricalQuotes, FinancialReportsQuarterCashFlowDirect, FinancialReportsQuarterCashFlowIndirect, FinancialReportsQuarterIncomeStatement, FinancialReportsQuarterBalanceSheet, FundamentalQuarter, VNINDEXHistoricalQuotes, Fundamental_1, Fundamental_2, Dividend
# financial_report_list = [
#     FinancialReportsQuarterBalanceSheet,
#     FinancialReportsQuarterIncomeStatement,
#     FinancialReportsQuarterCashFlowDirect,
#     FinancialReportsQuarterCashFlowIndirect,
# ]
# historical =[
#     HistoricalQuotes
# ]
# fundamental =[
#     Fundamental_1,
#     Fundamental_2
# ]
# for model_cls in fundamental:
#     load_main(
#         model_cls=model_cls
#     )
load_main(model_cls = Dividend)


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
from utils.minio_maintenance import LakehouseMaintenance
from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
with LakehouseMaintenance(lake_client=LakeHouseClient(), pg_client=PostgresClient()) as maintenance:
    maintenance.minio_maintenance()


# %%
import polars as pl
import json
raw_data ="""[
    {
        "year": 2023,
        "cashDividend": 0.0,
        "stockDividend": 18.0,
        "totalAssets": 630500685000000.0,
        "stockHolderEquity": 50098280000000.0
    },
    {
        "year": 2024,
        "cashDividend": 500.0,
        "stockDividend": 0.0,
        "totalAssets": 747478069000000.0,
        "stockHolderEquity": 58067344000000.0
    },
    {
        "year": 2025,
        "cashDividend": 500.0,
        "stockDividend": 24.0,
        "totalAssets": 892008709000000.0,
        "stockHolderEquity": 68130938000000.0
    },
    {
        "year": 2026,
        "cashDividend": 0.0,
        "stockDividend": 0.0,
        "totalAssets": null,
        "stockHolderEquity": null
    }
]"""

full_data =[{
    "ticker": "AAA",
    "url": "https://example.com/AAA/financials",
    "data": json.loads(raw_data)
},
{
    "ticker": "BBB",
    "url": "https://example.com/BBB/financials",
    "data": json.loads(raw_data)
}]
df = pl.DataFrame(full_data)
df = df.explode("data").unnest("data")
print(df)



# %%

# %%
from airflow.sdk import dag, task
from datetime import datetime
import os
from airflow.providers.docker.operators.docker import DockerOperator # type: ignore

DOCKER_NETWORK = "my_de_project_monitoring_net"
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

jdbc_args = [
        "--jdbc-url", "jdbc:postgresql://postgres:5432/platform_db?currentSchema=nessie_gc",
        "--jdbc-user", POSTGRES_USER,
        "--jdbc-password", POSTGRES_PASSWORD
    ]

command=["create-sql-schema"] + jdbc_args + ["--jdbc-schema", "DROP_AND_CREATE"]
print(command)

# %%

# %%
