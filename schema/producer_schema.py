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
from common.clients.postgres_client import PostgresClient
from common.clients.lakehouse_client import LakeHouseClient
from common.core.time_utils import get_target_anchor, get_fallback_year
import math


logger = setup_logger(component="schema")
class BaseMetadata(BaseModel,ABC):
    """Lớp cha để định danh mọi loại Metadata trong hệ thống"""
    model_config = ConfigDict(
        strict=True, 
        str_strip_whitespace=True, 
        extra='ignore'
    )
    
    ticker_list_mode: Literal['fundamental', 'other_data', 'vnindex'] = "other_data" # default, có thể override ở class con nếu muốn
    batch_size: int 
    table_name_postgres: str = "ingestion_kafka_state"
    table_watermark_name_postgres: Optional[str] = "ingestion_watermark"
    url_template: str
    data_type : str
    bronze_layer_name: str 
    
    # 1. static fields (lấy thẳng từ YAML, không động theo từng mã)
    source: str = Field(default_factory=lambda: os.getenv("MY_SOURCE", "UNKNOWN"))
    created_at_ts: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ticker: str = Field(default="") # field động, sẽ được update theo từng mã trong quá trình tạo message Kafka
    
    def __enter__(self):
        """Khởi tạo tiêu chuẩn cho mọi class con"""
        logger.info(f"🚪 [BASE] Bắt đầu phiên làm việc với topic: {self.data_type}")
        return self
    
    def _generate_kafka_message(self, 
                                 ticker_list: list[str], 
                                 metadata_manager: MetadataManager, 
                                 batch_id: str ):       
        """Hàm trừu tượng, bắt buộc class con phải implement để tạo message Kafka"""
    pass

    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        if not isinstance(api_data, list):
            raise ValueError(f"Dữ liệu Historical phải là List, nhưng lại nhận được: {type(api_data)}")
        processed_time = datetime.now(ZoneInfo("UTC"))
        return {
                **msg,  
                "data": api_data, 
                "message_processed_time": processed_time                   
            }

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1. Logic xử lý lỗi chung (Nếu có lỗi văng ra trong khối with)
        if exc_type:
            logger.error(f"❌ Luồng {self.__class__.__name__} bị gián đoạn do lỗi: {exc_val}")
            return False
        logger.info(f"đóng kết nối thành công cho class dữ liệu {self.__class__.__name__}")
              
    
    @property
    def iceberg_schema(self) -> Schema:
        """Tự động tra sổ lấy Schema dựa vào data_type của Class con"""
        if self.bronze_layer_name not in TABLE_REGISTRY:
            raise ValueError(f"Chưa định nghĩa Schema cho bảng: {self.bronze_layer_name}")
        return TABLE_REGISTRY[self.bronze_layer_name]["schema"]

    @property
    def iceberg_partition_spec(self) -> PartitionSpec:
        """Tự động tra sổ lấy Partition Spec dựa vào data_type của Class con"""
        if self.bronze_layer_name not in TABLE_REGISTRY:
            raise ValueError(f"Chưa định nghĩa Partition cho bảng: {self.bronze_layer_name}")
        return TABLE_REGISTRY[self.bronze_layer_name]["partition_spec"]

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
            
            # Bọc cái message_bytes vào trong 1 cái list
            
            yield ticker, [message_bytes]

    def _create_kafka_message(self, ticker: str, batch_id: str) -> Tuple[str, bytes]:
        """Tạo payload và trả về (ticker, bytes)"""
        # model_copy update data động cực nhanh mà không chạy lại validation rule
        updated_instance = self.model_copy(update={"ticker": ticker,
                                                   "batch_id": batch_id
                                                   })
        
        message_bytes = updated_instance.model_dump_json(
            include={ # những field data nào sẽ được đưa vào message Kafka, có thể tùy chỉnh ở đây nếu muốn
                "ticker", 
                "batch_id", 
                "data_type", 
                "source", 
                "created_at_ts", 
                "url" 
            },
            ensure_ascii=False
        ).encode('utf-8')
        
        return ticker, message_bytes
    
    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        processed_time = datetime.now(ZoneInfo("UTC"))
        msg.update({
            "data": json.dumps(api_data, ensure_ascii=False),
            "message_processed_time": processed_time
            })
        return msg
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> pa.Table:
        if not buffer:
            raise ValueError("Danh sách records trống.")
        
        lf = pl.LazyFrame(buffer)
        df = lf.collect()
        df = df.with_columns(
            pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time"),
            pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC")
        )
        return df.to_arrow()
    
class Fundamental_1(Fundamental):
    data_type: str = "fundamental_1"
    bronze_layer_name: str = "fundamental_1"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/fundamental"
    
class Fundamental_2(Fundamental):
    data_type: str = "fundamental_2"
    bronze_layer_name: str = "fundamental_2"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}"
 
 
 
 
 
    
class HistoricalQuotes(BaseMetadata):
    batch_size: int = 10
    default_limit: int = 500
    default_offset: int = 0
    data_type: str = "historical_quotes"
    bronze_layer_name: str = "historical_quotes"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/historical-quotes?startDate={start_date}&endDate={end_date}&offset={offset}&limit={limit}"
    start_date: date = Field(default_factory=lambda: date(get_fallback_year(), 1, 1))
    end_date: date = Field(default_factory=lambda: date.today() - timedelta(days=1))
    
    
    
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
            raise ValueError(f"⚠️ [{self.data_type}] Bắt buộc phải có tên bảng Watermark để chạy loại dữ liệu này!")
        metadata_manager.reconcile_watermark_from_lakehouse(config=self)
        metadata_manager.sync_watermark(config = self) 
        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(ticker, config = self) 
            # 1. Tính số ngày theo lịch (Calendar Days)
            calendar_days = (self.end_date - start_date).days 
            if calendar_days < 0: 
                continue
            estimated_trading_days = int(calendar_days * (250 / 365)) + 20
            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                start_date=start_date,
                offset=self.default_offset,
                batch_id = batch_id,
                limit=estimated_trading_days
            )
            # Nhét vào giỏ
            yield ticker, [message_bytes]

    def _create_kafka_message(self, ticker: str, start_date: date, offset: int, batch_id: str, limit: int) -> Tuple[str, bytes]:
        
        updated_instance = self.model_copy(update={
            "ticker": ticker, 
            "start_date": start_date,
            "default_limit": limit,
            "default_offset": offset,
            "batch_id": batch_id

        })
        
        message_bytes = updated_instance.model_dump_json(
            include={ 
                "ticker", "batch_id", "data_type", "source", 
                "created_at_ts", "url" 
            },
            ensure_ascii=False
        ).encode('utf-8')
        return ticker, message_bytes
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        df = lf.explode("data")
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
    ticker_list_mode: Literal['fundamental', 'other_data', 'vnindex'] = "vnindex"
    data_type : str = "vnindex_historical_quotes"
    

class FinancialReportsQuarter(BaseMetadata):
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
            raise ValueError(f"⚠️ [{self.data_type}] Bắt buộc phải có tên bảng Watermark để chạy loại dữ liệu này!")
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
                    f"[FINANCIAL_REPORTS][SKIP_TICKER] Bỏ qua mã {ticker:<5} | "
                    f"Lý do: Watermark DB (Q{start_quarter}/{start_year}: {start_date}) >= Target (Q{self.target_quarter}/{self.target_year}) | "
                    f"Calc_Limit: {limit} <= 0"
                )
                continue

            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                limit=limit,
                type_id=self.type_id,
                batch_id=batch_id
            )
            # Nhét vào giỏ
            yield ticker, [message_bytes]
        
    
    def _create_kafka_message(self, ticker: str, limit: int, type_id: int, batch_id: str) -> Tuple[str, bytes]:
        
        updated_instance = self.model_copy(update={
            "ticker": ticker, 
            "default_limit": limit,
            "type_id": type_id,
            "batch_id": batch_id
        })
        
        message_bytes = updated_instance.model_dump_json(
            include={ 
                "ticker", "batch_id", "data_type", "source", 
                "created_at_ts", "url" 
            },
            ensure_ascii=False
        ).encode('utf-8')
        return ticker, message_bytes
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        df = lf.explode("data").unnest("data").explode("values").unnest("values").select([
                pl.col("ticker"),
                pl.col("batch_id"),
                pl.col("data_type"),
                pl.col("source"),
                pl.col("url"),
                pl.col("id").alias("indicator_id").cast(pl.Int32),     # Lấy cột id và đổi tên
                pl.col("name").alias("indicator_name"), # Lấy cột name và đổi tên
                pl.col("year").cast(pl.Int32),
                pl.col("quarter").cast(pl.Int32),
                pl.col("value").cast(pl.Float64),
                pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
                pl.col("message_processed_time"),
                pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
    ])
        return df.collect().to_arrow()

class FundamentalQuarter(FinancialReportsQuarter):
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
            raise ValueError(f"⚠️ [{self.data_type}] Bắt buộc phải có tên bảng Watermark để chạy loại dữ liệu này!")
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
                    f"[FINANCIAL_REPORTS][SKIP_TICKER] Bỏ qua mã {ticker:<5} | "
                    f"Lý do: Watermark DB (Q{start_quarter}/{start_year}: {start_date}) >= Target (Q{self.target_quarter}/{self.target_year}) | "
                    f"Calc_Limit: {limit} <= 0"
                )
                continue

            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                limit=limit,
                batch_id=batch_id
            )
            # Nhét vào giỏ
            yield ticker, [message_bytes]
    
    def _create_kafka_message(self, ticker: str, limit: int, batch_id: str) -> Tuple[str, bytes]:
        
        updated_instance = self.model_copy(update={
            "ticker": ticker, 
            "default_limit": limit,
            "batch_id": batch_id
        })
        
        message_bytes = updated_instance.model_dump_json(
            include={ 
                "ticker", "batch_id", "data_type", "source", 
                "created_at_ts", "url" 
            },
            ensure_ascii=False
        ).encode('utf-8')
        return ticker, message_bytes
    

    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        # Khởi tạo LazyFrame từ list các message Kafka
        lf = pl.LazyFrame(buffer)
        
        df = (
            lf.explode("data")   # 1. Tách list các quý thành từng dòng
            .unnest("data")    # 2. Đập vỡ vỏ bọc để lòi ra year, quarter, financialValues...
            .select([
                # --- Các cột Metadata (Giữ nguyên như schema cũ của bác) ---
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
        """
        Generate messages for yearly financial reports.
        Logic: target_year - start_year + 1
        """
        if not self.table_watermark_name_postgres:
            raise ValueError(f"⚠️ [{self.data_type}] Bắt buộc phải có tên bảng Watermark để chạy loại dữ liệu này!")
        # 1. Đồng bộ danh sách ticker trước
        metadata_manager.reconcile_watermark_from_lakehouse(config=self)
        metadata_manager.sync_watermark(
            config=self
        )

        for ticker in ticker_list:
            # 2. Lấy start_date từ Watermark (vốn là updated_at của mẻ thành công cuối)
            start_date = metadata_manager._get_smart_start_date(
                ticker=ticker, 
                config=self
            )
            
            start_year = start_date.year
            
            # 3. Tính toán Limit theo công thức "Thần toán" của ông giáo
            # Ví dụ: target_year = 2025, start_year = 2020 -> limit = 6
            limit = (self.target_year - start_year) + 1
            
            # 4. Kiểm tra điều kiện dừng (Nếu đã cập nhật đến năm hiện tại rồi)
            if limit <= 0:
                logger.debug(f"⏭️ [{self.data_type.upper()}] {ticker} is up to date (Start: {start_year}, Target: {self.target_year}).")
                continue

            # 5. Buffer nhẹ (Tùy chọn): Nếu ông giáo muốn lấy thừa 1 năm cho chắc
            # limit += 1 

            # 6. Tạo tin nhắn và yield
            ticker, message_bytes = self._create_kafka_message(
                ticker=ticker,
                limit=limit,
                batch_id=batch_id,
                type_id=self.type_id
            )
            yield ticker, [message_bytes]
    
    def _create_kafka_message(self, ticker: str, limit: int, batch_id: str, type_id: int) -> Tuple[str, bytes]:
        
        updated_instance = self.model_copy(update={
            "ticker": ticker, 
            "default_limit": limit,
            "batch_id": batch_id, 
            "type_id": type_id
        })
            
        message_bytes = updated_instance.model_dump_json(
            include={ 
                "ticker", "batch_id", "data_type", "source", 
                "created_at_ts", "url" 
            },
            ensure_ascii=False
        ).encode('utf-8')
        return ticker, message_bytes
    
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        df = lf.explode("data").unnest("data").explode("values").unnest("values").select([
                pl.col("ticker"),
                pl.col("batch_id"),
                pl.col("data_type"),
                pl.col("source"),
                pl.col("url"),
                pl.col("id").alias("indicator_id").cast(pl.Int32),     # Lấy cột id và đổi tên
                pl.col("name").alias("indicator_name"), # Lấy cột name và đổi tên
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
    