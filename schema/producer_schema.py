"""
This module contains specific data classes defining attributes and methods
tailored for each data type in the pipeline.
By using Pydantic for robust data validation and Object-Oriented Programming (OOP) inheritance, the 
architecture strictly adheres to the DRY (Don't Repeat Yourself) principle. 

This design ensures easy readability, simple debugging, and quick extensibility 
for integrating new data sources in the future.
"""

from abc import ABC, abstractmethod
import json
import os
import polars as pl
from pydantic import AliasPath, BaseModel, ConfigDict, computed_field, Field, ValidationError
from typing import Any, ClassVar, Dict, List, Tuple, TypeVar, Set, Iterable, Optional, Literal
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import uuid
from common.core.logger_config import setup_logger
import pyarrow as pa
from schema.schema_metadata import TABLE_REGISTRY 
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from common.infrastructure.metadata_manager import MetadataManager
from common.core.time_utils import get_target_anchor, get_fallback_year
import math


logger = setup_logger(component="infrastructure")
class BaseMetadata(BaseModel,ABC):
    """
    Abstract Base Class defining the blueprint for all metadata schemas.
    It manages shared attributes, Kafka message generation, and data transformation rules.
    """
    model_config = ConfigDict(
        strict=True, 
        str_strip_whitespace=True, 
        extra='ignore'
    )
    
    ticker_list_mode: Literal['fundamental', 'other_data', 'vnindex'] = "other_data" 
    batch_size: int 
    table_name_postgres: str = "ingestion_kafka_state"
    table_watermark_name_postgres: Optional[str] = "ingestion_watermark"
    url_template: str
    data_type : str
    bronze_layer_name: str 
    source: str = Field(default_factory=lambda: os.getenv("MY_SOURCE", "UNKNOWN"))
    created_at_ts: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ticker: str = Field(default="") # Dynamic field (Updated per ticker during message generation)
    
    def __enter__(self):
        logger.info(f"Initiating pipeline session. Data Type: '{self.data_type}'.")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error(f"Pipeline session interrupted for '{self.__class__.__name__}'. Error: {exc_val}", exc_info=True)
            return False
        logger.info(f"Pipeline session closed successfully. Data Type: '{self.data_type}'.")
              
    
    def _create_kafka_message(self, ticker: str, batch_id: str, **kwargs) -> Tuple[str, bytes]:
        """
        A unified helper method used by all child classes to generate a Kafka payload.
        """
        # STEP 1: Prepare the dynamic fields to update
        update_data = {"ticker": ticker, "batch_id": batch_id}
        if kwargs:
            update_data.update(kwargs)
            
        # STEP 2: Create a lightweight copy of the model with the new values
        updated_instance = self.model_copy(update=update_data)
        
        # STEP 3: Serialize to JSON bytes (filtering only required tracking fields)
        message_bytes = updated_instance.model_dump_json(
            include={ 
                "ticker", "batch_id", "data_type", "source", 
                "created_at_ts", "url" 
            },
            ensure_ascii=False
        ).encode('utf-8')
        
        return ticker, message_bytes
    
    def _generate_kafka_message(self, 
                                 ticker_list: list[str], 
                                 metadata_manager: MetadataManager, 
                                 batch_id: str ):       
        """
        Abstract method to generate Kafka claim-check messages.
        Must be implemented by child classes to yield tuples of (ticker, [message_bytes]).
        """
    pass

    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        """
        Default transformation logic. Appends the heavy API payload to the Kafka message.
        """
        if not isinstance(api_data, list):
            raise TypeError(f"Expected API data to be a List, but received: {type(api_data)}")
        processed_time = datetime.now(ZoneInfo("UTC"))
        return {
                **msg,  
                "data": api_data, 
                "message_processed_time": processed_time                   
            }

    
    @property
    def iceberg_schema(self) -> Schema:
        """Retrieves the exact Iceberg schema from the global registry."""
        if self.bronze_layer_name not in TABLE_REGISTRY:
            raise ValueError(f"Schema not found in registry for table: '{self.bronze_layer_name}'")
        return TABLE_REGISTRY[self.bronze_layer_name]["schema"]

    @property
    def iceberg_partition_spec(self) -> PartitionSpec:
        """Retrieves the Iceberg partition specification from the global registry."""
        if self.bronze_layer_name not in TABLE_REGISTRY:
            raise ValueError(f"Partition Spec not found in registry for table: '{self.bronze_layer_name}'")
        return TABLE_REGISTRY[self.bronze_layer_name]["partition_spec"]
# ==========================================
# Specific Data Types classes (Inherit from BaseMetadata)
# ==========================================
class Fundamental(BaseMetadata):
    batch_size : int = 100
    ticker_list_mode : str = "fundamental"
    table_watermark_name_postgres: Optional[str] = None

    @computed_field
    def url(self) -> str:
        return self.url_template.format(ticker=self.ticker, source=self.source)
    
    def _generate_kafka_message(
    self, 
    batch_id: str,
    ticker_list: Iterable[str],
    metadata_manager: MetadataManager | None = None,
) -> Iterable[Tuple[str, List[bytes]]]:
        """Chỉ gọi hàm nhân bản cực nhẹ của object."""
        for ticker in ticker_list:
            ticker, message_bytes = self._create_kafka_message(ticker=ticker, batch_id=batch_id)
            yield ticker, [message_bytes]
    
    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        # safely convert the dict/list API data into a JSON string
        processed_time = datetime.now(ZoneInfo("UTC"))
        msg.update({
            "data": json.dumps(api_data, ensure_ascii=False),
            "message_processed_time": processed_time
            })
        return msg
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> pa.Table:
        # STEP 1: Parse the memory buffer into a Polars LazyFrame
        # STEP 2: Add ingestion timestamps
        # STEP 3: Execute the computation and convert to PyArrow Table
        if not buffer:
            raise ValueError("Data buffer is empty. Cannot build PyArrow payload.")
        
        lf = pl.LazyFrame(buffer)
        df = (
            lf.select([
                pl.col("ticker"),
                pl.col("data"), 
                pl.col("batch_id"),
                pl.col("data_type"),
                pl.col("source"),
                pl.col("url"),
                pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
                pl.col("message_processed_time"), 
                pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
            ])
        )
        
        return df.collect().to_arrow()
    
class Fundamental_1(Fundamental):
    """
    Handles core company information data.
    Includes data points such as company type, outstanding shares, 
    3-month average trading volume, and foreign ownership ratio,...
    """
    data_type: str = "fundamental_1"
    bronze_layer_name: str = "fundamental_1"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/fundamental"
    
class Fundamental_2(Fundamental):
    """
    Handles exchange-related company information.
    Includes data points such as the listed exchange name and the current listing status.
    """
    data_type: str = "fundamental_2"
    bronze_layer_name: str = "fundamental_2"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}"
 
    
class HistoricalQuotes(BaseMetadata):
    """
    Handles historical price and volume data for individual tickers.
    """
    batch_size: int = 10
    default_limit: int = 500
    default_offset: int = 0
    data_type: str = "historical_quotes"
    bronze_layer_name: str = "historical_quotes"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/historical-quotes?startDate={start_date}&endDate={end_date}&offset={offset}&limit={limit}"
    start_date: date = Field(default_factory=lambda: date(get_fallback_year(), 1, 1))
    end_date: date = Field(default_factory=lambda: date.today() - timedelta(days=1))
    
    
    # after calling `create_kafka_message`, the `url` field will be dynamically computed based on the updated `ticker`and other data fields for each message
    @computed_field
    def url(self) -> str:
        return self.url_template.format(
            source=self.source, 
            ticker=self.ticker, 
            start_date=self.start_date, 
            end_date=self.end_date,
            offset=self.default_offset, 
            limit=self.default_limit
        )
    
    def _generate_kafka_message(self, ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
        if not self.table_watermark_name_postgres:
            raise ValueError(f"[{self.data_type}] Watermark table name is required to run this data type.")
        # STEP 1: update the list of tickers with latest reference ticker list from lakehouse
        # STEP 2: Synchronize watermarks to determine the latest date to fetch data for each tickers.
        metadata_manager.reconcile_watermark_from_lakehouse(config=self)
        metadata_manager.sync_watermark(config = self) 

        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(ticker, config = self) 
            #  Calculate calendar days difference
            calendar_days = (self.end_date - start_date).days 
            if calendar_days < 0: 
                continue
            # STEP 3: Estimate trading days to set API limits (approx. 250 trading days per year)
            estimated_trading_days = int(calendar_days * (250 / 365)) + 20
            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                start_date=start_date,
                offset=self.default_offset,
                batch_id = batch_id,
                default_limit=estimated_trading_days
            )
            yield ticker, [message_bytes]
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        # STEP 1: Explode the list of historical records into individual rows
        df = lf.explode("data")
        # STEP 2: Extract nested fields and assign appropriate data types
        df = df.with_columns([
            pl.col("data").struct.field("date").cast(pl.Datetime).dt.date().alias("event_date"),
            pl.col("data").struct.field("date").cast(pl.Datetime).dt.year().cast(pl.Int32).alias("year"),
            pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
            pl.col("data").struct.json_encode().alias("data"),
            pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
        ]
        )
        return df.collect().to_arrow()

class VNINDEXHistoricalQuotes(HistoricalQuotes):
    """Handles historical quotes specifically for the VNINDEX."""
    ticker_list_mode: Literal['fundamental', 'other_data', 'vnindex'] = "vnindex"
    data_type : str = "vnindex_historical_quotes"
    

class FinancialReportsQuarter(BaseMetadata):
    """
    Base class for handling quarterly financial report data for individual tickers.
    """
    bronze_layer_name: str = "financial_reports_quarter"
    batch_size: int = 10
    default_limit : int = 0
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/full-financial-reports?type={type_id}&year={year}&quarter={quarter}&limit={limit}"
    target_year: int = Field(default_factory=lambda: get_target_anchor("quarter")[0])
    target_quarter: int = Field(default_factory=lambda: get_target_anchor("quarter")[1])
    type_id : int = Field(default=0, description="""
                          1: Báo cáo cân đối kế toán(Quarterly)
                          2: Báo cáo kết quả kinh doanh(Quarterly)
                          3: Báo cáo lưu chuyển tiền tệ trực tiếp(Quarterly)
                          4: Báo cáo lưu chuyển tiền tệ gián tiếp(Quarterly)
                          """)
    
    @computed_field
    def url(self) -> str:
        return self.url_template.format(
            source=self.source, 
            ticker=self.ticker, 
            year=self.target_year, 
            quarter=self.target_quarter,
            limit=self.default_limit,
            type_id=self.type_id
        )
    
    def _generate_kafka_message(self,  ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
        if not self.table_watermark_name_postgres:
            raise ValueError(f"[{self.data_type}] Watermark table name is required to run this data type.")
        
        
        metadata_manager.reconcile_watermark_from_lakehouse(config=self)
        metadata_manager.sync_watermark(config=self) 
        
        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(ticker, config=self) 
            start_year = start_date.year
            start_quarter = math.ceil(start_date.month / 3)
            limit = (self.target_year - start_year) * 4 + (self.target_quarter - start_quarter) 
            limit += 2
            if limit <= 0:
                logger.warning(
                    f"[SKIP_TICKER] Ticker {ticker:<5} | "
                    f"Reason: Watermark (Q{start_quarter}/{start_year}) >= Target (Q{self.target_quarter}/{self.target_year}) | "
                    f"Calculated Limit: {limit}"
                )
                continue

            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                default_limit=limit,
                type_id=self.type_id,
                batch_id=batch_id
            )
            yield ticker, [message_bytes]
        
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        # STEP 1: Flatten nested JSON structures to extract individual indicator values
        df = lf.explode("data").unnest("data").explode("values").unnest("values").select([
                pl.col("ticker"),
                pl.col("batch_id"),
                pl.col("data_type"),
                pl.col("source"),
                pl.col("url"),
                pl.col("id").alias("indicator_id").cast(pl.Int32),     
                pl.col("name").alias("indicator_name"), 
                pl.col("year").cast(pl.Int32),
                pl.col("quarter").cast(pl.Int32),
                pl.col("value").cast(pl.Float64),
                pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
                pl.col("message_processed_time"),
                pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
    ])
        return df.collect().to_arrow()

class FundamentalQuarter(FinancialReportsQuarter):
    """This class handles quarterly fundamental metrics."""
    data_type: str = "fundamental_quarter"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/financial-data?type=Q&count={limit}"
    bronze_layer_name: str = "fundamental_quarter"
    
    
    @computed_field
    def url(self) -> str:
        return self.url_template.format(
            source=self.source, 
            ticker=self.ticker, 
            limit=self.default_limit,
        )
    
    def _generate_kafka_message(self,  ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
        if not self.table_watermark_name_postgres:
           raise ValueError(f"[{self.data_type}] Watermark table name is required to run this data type.")
       
        metadata_manager.reconcile_watermark_from_lakehouse(config=self)
        metadata_manager.sync_watermark(config=self) 

        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(ticker, config=self) 
            start_year = start_date.year
            start_quarter = math.ceil(start_date.month / 3)
            limit = (self.target_year - start_year) * 4 + (self.target_quarter - start_quarter) 
            limit += 2
            if limit <= 0:
                logger.warning(
                    f"[SKIP_TICKER] Ticker {ticker:<5} | "
                    f"Reason: Watermark (Q{start_quarter}/{start_year}) >= Target (Q{self.target_quarter}/{self.target_year}) | "
                    f"Calculated Limit: {limit}"
                )
                continue

            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                default_limit=limit,
                batch_id=batch_id
            )
       
            yield ticker, [message_bytes]
    

    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        # Khởi tạo LazyFrame từ list các message Kafka
        lf = pl.LazyFrame(buffer)
        
        df = (
            lf.explode("data")   # STEP 1: Split quarters into individual rows
            .unnest("data")    # STEP 2: Explode the nested structure to access year, quarter, financialValues...
            .select([
                pl.col("ticker"),
                pl.col("batch_id"),
                pl.col("data_type"),
                pl.col("source"),
                pl.col("url"),
                pl.col("year").cast(pl.Int32),
                pl.col("quarter").cast(pl.Int32),
                pl.col("financialValues").struct.json_encode().alias("value"),
                pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
                pl.col("message_processed_time"),
                pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
            ])
        )
        return df.collect().to_arrow()
    
class FinancialReportsQuarterBalanceSheet(FinancialReportsQuarter):
    data_type: str = "balance_sheet_quarter"
    type_id: int = 1

class FinancialReportsQuarterIncomeStatement(FinancialReportsQuarter):
    data_type: str = "income_statement_quarter"
    type_id: int = 2

class FinancialReportsQuarterCashFlowDirect(FinancialReportsQuarter):
    data_type: str = "cash_flow_direct_quarter"
    type_id: int = 3

class FinancialReportsQuarterCashFlowIndirect(FinancialReportsQuarter):
    data_type: str = "cash_flow_indirect_quarter"
    type_id: int = 4
    

class FinancialReportsYear(BaseMetadata):
    """
    Handles yearly financial reports data extraction.
    """
    bronze_layer_name: str = "financial_reports_year"
    batch_size: int = 10
    default_limit : int = 0
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/full-financial-reports?type={type_id}&year={year}&quarter=0&limit={limit}"
    target_year: int = Field(default_factory=lambda: get_target_anchor("year")[0])
    type_id : int = Field(default=0, description="""
                          1: Báo cáo cân đối kế toán(Quarterly)
                          2: Báo cáo kết quả kinh doanh(Quarterly)
                          3: Báo cáo lưu chuyển tiền tệ trực tiếp(Quarterly)
                          4: Báo cáo lưu chuyển tiền tệ gián tiếp(Quarterly)
                          """)
    
    @computed_field
    def url(self) -> str:
        return self.url_template.format(
            source=self.source, 
            ticker=self.ticker, 
            year=self.target_year, 
            limit=self.default_limit,
            type_id=self.type_id
        )
        
    def _generate_kafka_message(self, ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str) -> Iterable[Tuple[str, List[bytes]]]:
        if not self.table_watermark_name_postgres:
           raise ValueError(f"[{self.data_type}] Watermark table name is required to run this data type.")
       
        metadata_manager.reconcile_watermark_from_lakehouse(config=self)
        metadata_manager.sync_watermark(config=self)
        
        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(
                ticker=ticker, 
                config=self
            )
            
            start_year = start_date.year
        
            limit = (self.target_year - start_year) + 1
            
            if limit <= 0:
                logger.debug(f"⏭️ [{self.data_type.upper()}] {ticker} is up to date (Start: {start_year}, Target: {self.target_year}).")
                continue



            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                default_limit=limit,
                batch_id=batch_id,
                type_id=self.type_id
            )
            yield ticker, [message_bytes]
    
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        df = lf.explode("data").unnest("data").explode("values").unnest("values").select([
                pl.col("ticker"),
                pl.col("batch_id"),
                pl.col("data_type"),
                pl.col("source"),
                pl.col("url"),
                pl.col("id").alias("indicator_id").cast(pl.Int32),     
                pl.col("name").alias("indicator_name"),
                pl.col("year").cast(pl.Int32),
                pl.col("quarter").cast(pl.Int32), 
                pl.col("value").cast(pl.Float64),
                pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
                pl.col("message_processed_time"),
                pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
    ])
        return df.collect().to_arrow()
    
    
class FinancialReportsYearBalanceSheet(FinancialReportsYear):
    data_type: str = "balance_sheet_year"
    type_id: int = 1

class FinancialReportsYearIncomeStatement(FinancialReportsYear):
    data_type: str = "income_statement_year"
    type_id: int = 2

class FinancialReportsYearCashFlowDirect(FinancialReportsYear):
    data_type: str = "cash_flow_direct_year"
    type_id: int = 3

class FinancialReportsYearCashFlowIndirect(FinancialReportsYear):
    data_type: str = "cash_flow_indirect_year"
    type_id: int = 4
    
class Dividend(BaseMetadata):
    """
    Handles dividend data extraction for individual tickers.
    """
    batch_size: int = 10
    default_limit : int = 0
    data_type: str = "dividend"
    bronze_layer_name: str = "dividend_year"
    table_watermark_name_postgres: Optional[str] = None
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/dividends?count={limit}"
    start_date: date = Field(default_factory=lambda: date(get_fallback_year(), 1, 1))
    end_date: date = Field(default_factory=lambda: date.today() - timedelta(days=1))
    
    @computed_field
    def url(self) -> str:
        return self.url_template.format(
            source=self.source, 
            ticker=self.ticker, 
            limit=self.default_limit
        )

    def _generate_kafka_message(self, ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str) -> Iterable[Tuple[str, List[bytes]]]:
        for ticker in ticker_list:
            limit = (date.today().year - self.start_date.year) + 1

            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                default_limit=limit,
                batch_id=batch_id,
            )
            yield ticker, [message_bytes]
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> pa.Table:
        """
        Converts the message buffer into a PyArrow Table using Polars LazyFrame for optimized execution.
        """
        if not buffer:
            raise ValueError("Data buffer is empty. Cannot build PyArrow payload.")
        
        lf = pl.LazyFrame(buffer)
        df = (
            lf.explode("data")
              .select([
                  pl.col("ticker"),
                  pl.col("data").struct.field("year").cast(pl.Int32).alias("year"),
                  pl.col("data").struct.json_encode().alias("data"),
                  pl.col("batch_id"),
                  pl.col("data_type"),
                  pl.col("source"),
                  pl.col("url"),
                  pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
                  pl.col("message_processed_time"),
                  pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
              ])
        )
        
        return df.collect().to_arrow()