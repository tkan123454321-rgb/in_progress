import polars as pl
from utils.lakehouse_connection import LakeHouseClient
from pyiceberg.exceptions import *
from utils.logger_config import setup_logger
logger = setup_logger(component="utils")

class LakehouseReader:
    """
    Class chuyên dụng để ĐỌC dữ liệu.
    Dùng cho các job ETL, Enrichment, Reporting.
    """
    def __init__(self):
        self.infra = LakeHouseClient()
        self.catalog = self.infra.catalog
    
    def _get_ticker_list_raw(self):
        try:
            table = "silver.silver_dim_company"
            tbl = self.catalog.load_table(table)
            tbl = tbl.scan(selected_fields=("ticker",)).to_polars()
            result = set(tbl["ticker"].to_list())
            logger.info(f"Successfully fetch {len(result)} tickers")
            return result
        except NoSuchTableError as e:
            logger.error(f"Table {table} does not exist.")
            raise e
    
        