from abc import ABC, abstractmethod
import json
import os
import polars as pl
from pydantic import AliasPath, BaseModel, ConfigDict, computed_field, Field, ValidationError
from typing import Any, ClassVar, Dict, List, Tuple, TypeVar, Set, Iterable
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import uuid
from utils.logger_config import setup_logger
import pyarrow as pa
from schema.schema_metadata import TABLE_REGISTRY 
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from utils.metadata_manager import MetadataManager
from utils.postgres_client import PostgresClient
from utils.lakehouse_client import LakeHouseClient
from utils.other_utils import get_target_anchor
from vnstock import Listing
import math


logger = setup_logger(component="schema")
class BaseMetadata(BaseModel,ABC):
    """Lớp cha để định danh mọi loại Metadata trong hệ thống"""
    model_config = ConfigDict(
        strict=True, 
        str_strip_whitespace=True, 
        extra='ignore'
    )
    
    
    ticker_list_mode: str = "other_data" # default, có thể override ở class con nếu muốn
    batch_size: int
    table_name_postgres: str 
    topic: str
    url_template: str
    
    # 1. static fields (lấy thẳng từ YAML, không động theo từng mã)
    source: str = Field(default_factory=lambda: os.getenv("MY_SOURCE", "UNKNOWN"))
    created_at_ts: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    encountered_tickers: Set[str] = Field(default_factory=set, exclude=True)
    unsuccessful_tickers: Set[str] = Field(default_factory=set, exclude=True)
    
    ticker: str = "UNKNOWN"
    
    def __enter__(self):
        """Khởi tạo tiêu chuẩn cho mọi class con"""
        logger.info(f"🚪 [BASE] Bắt đầu phiên làm việc với topic: {self.topic}")
        return self
    
    def _generate_metadata_kafka(self, 
                                 ticker_list: list[str], 
                                 metadata_manager: MetadataManager, 
                                 batch_id: str ):       
        """Hàm trừu tượng, bắt buộc class con phải implement để tạo message Kafka"""
    pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1. Logic xử lý lỗi chung (Nếu có lỗi văng ra trong khối with)
        if exc_type:
            logger.error(f"🔥 [{self.topic.upper()}] Pipeline execution interrupted due to error: {exc_val}")
            # Vẫn cho code chạy tiếp xuống dưới để vét cạn những mã đã kịp thành công!

        # 2. Kiểm tra class con có dùng Watermark không (Ví dụ: Fundamental thì không)
        watermark_table = getattr(self, "table_watermark_name_postgres", None)
        if not watermark_table:
            logger.info(f"⏭️ [{self.topic.upper()}] Watermark table not configured. Skipping commit phase.")
            return

        # 3. Phép trừ tập hợp để chốt danh sách thành công
        successful_tickers = self.encountered_tickers - self.unsuccessful_tickers

        # 4. CHẶN ĐỨNG THÙNG RỖNG: Không có mã thành công thì nghỉ luôn cho khỏe Database
        if not successful_tickers:
            logger.warning(f"⚠️ [{self.topic.upper()}] No fully successful tickers found. Skipping watermark commit.")
            return

        # 5. Ghi Log thống kê (Stats) và tiến hành update Database
        logger.info(f"🔄 [{self.topic.upper()}] Committing watermark to table: {watermark_table}")
        logger.info(f"📊 [{self.topic.upper()}] Stats | Encountered: {len(self.encountered_tickers)} | Failed: {len(self.unsuccessful_tickers)} | Successful: {len(successful_tickers)}")
        
        try:
            with MetadataManager(pg_client=PostgresClient(), lake_client=LakeHouseClient()) as metadata_manager:
                metadata_manager._update_watermark(
                    table_name_watermark_postgres=watermark_table, 
                    successful_tickers=successful_tickers, 
                    batch_time=self.created_at_ts # Dùng mốc thời gian đã 'đóng băng' lúc khởi tạo class
                )
            logger.info(f"✅ [{self.topic.upper()}] Watermark commit successful.")
            
        except Exception as e:
            logger.error(f"❌ [{self.topic.upper()}] Database error during watermark commit on exit: {e}")
            raise e
    
    @property
    def iceberg_schema(self) -> Schema:
        """Tự động tra sổ lấy Schema dựa vào data_type của Class con"""
        if self.topic not in TABLE_REGISTRY:
            raise ValueError(f"Chưa định nghĩa Schema cho loại dữ liệu: {self.topic}")
        return TABLE_REGISTRY[self.topic]["schema"]

    @property
    def iceberg_partition_spec(self) -> PartitionSpec:
        """Tự động tra sổ lấy Partition Spec dựa vào data_type của Class con"""
        if self.topic not in TABLE_REGISTRY:
            raise ValueError(f"Chưa định nghĩa Partition cho loại dữ liệu: {self.topic}")
        return TABLE_REGISTRY[self.topic]["partition_spec"]

class OriginalTickerList(BaseMetadata):
    data_type: str = "original_ticker_list"
    table_name_postgres: str = "not_used"
    topic: str = "original_ticker_list"
    url_template: str = ""
    batch_size: int = 1000
    
    def _build_arrow_payload_lazy(self) -> pa.Table:
        listing = Listing(source='VCI')
        df = listing.symbols_by_industries()
        df = pl.from_pandas(df)
        df = df.with_columns(pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time"))
        return df.to_arrow()
    

class Fundamental(BaseMetadata):
    batch_size : int = 100
    ticker_list_mode : str = "fundamental"

    @computed_field
    def url(self) -> str:
        return self.url_template.format(ticker=self.ticker, source=self.source)
    
    def _generate_metadata_kafka(
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
    topic: str = "fundamental_1"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/fundamental"
    table_name_postgres: str = "ingestion_metadata_fundamental_1"

class Fundamental_2(Fundamental):
    data_type: str = "fundamental_2"
    topic: str = "fundamental_2"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}"
    table_name_postgres: str = "ingestion_metadata_fundamental_2"
    


class HistoricalQuotes(BaseMetadata):
    batch_size: int = 10
    default_limit: int = 500
    default_offset: int = 0
    table_name_postgres: str = "ingestion_metadata_historical_quotes"
    topic: str = "historical_quotes"
    data_type: str = "historical_quotes"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/historical-quotes?startDate={start_date}&endDate={end_date}&offset={offset}&limit={limit}"
    start_date: date = Field(default=date(2018, 1, 1))
    end_date: date = Field(default_factory=lambda: date.today() - timedelta(days=1))
    table_watermark_name_postgres : str = "ingestion_historical_quotes_watermark"
    
    
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
    
    def _generate_metadata_kafka(self, ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
        metadata_manager.sync_historical_watermark_tickers(table_name_watermark_postgres=self.table_watermark_name_postgres) # type: ignore
        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(ticker, table_name_watermark_postgres=self.table_watermark_name_postgres) # type: ignore
            # 1. Tính số ngày theo lịch (Calendar Days)
            calendar_days = (self.end_date - start_date).days 
            if calendar_days < 0: 
                continue
            estimated_trading_days = int(calendar_days * (250 / 365)) + 20
            batch_messages = []
            for current_offset in range(self.default_offset, estimated_trading_days, self.default_limit):
                ticker, message_bytes = self._create_kafka_message(
                    ticker=ticker,
                    start_date=start_date,
                    offset=current_offset,
                    batch_id = batch_id
                )
                # Nhét vào giỏ
                batch_messages.append(message_bytes)
            yield ticker, batch_messages

    def _create_kafka_message(self, ticker: str, start_date: date, offset: int, batch_id: str) -> Tuple[str, bytes]:
        
        updated_instance = self.model_copy(update={
            "ticker": ticker, 
            "start_date": start_date,
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
    
    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        if not isinstance(api_data, list):
            raise ValueError(f"Dữ liệu Historical phải là List, nhưng lại nhận được: {type(api_data)}")
        processed_time = datetime.now(ZoneInfo("UTC"))
        return {
                **msg,  
                "data": api_data, 
                "message_processed_time": processed_time                   
            }
    
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> Any:
        lf = pl.LazyFrame(buffer)
        df = lf.explode("data")
        df = df.with_columns([
            pl.col("data").struct.field("date").cast(pl.Datetime).dt.date().alias("event_date"),
            pl.col("created_at_ts").str.to_datetime(time_unit="us", time_zone="UTC"),
            pl.col("data").struct.json_encode().alias("data"),
            pl.lit(datetime.now(ZoneInfo("UTC"))).alias("bronze_ingested_time")
        ]
        )
        return df.collect().to_arrow()

    

class FinancialReports(BaseMetadata):
    batch_size: int = 10
    default_limit : int = 0
    table_name_postgres: str = "ingestion_metadata_financial_reports_quarter"
    table_watermark_name_postgres : str = "ingestion_financial_reports_watermark_quarter"
    topic: str = "financial_reports_quarter"
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
    
    def _generate_metadata_kafka(self,  ticker_list: list[str], metadata_manager: MetadataManager, batch_id: str
)-> Iterable[Tuple[str, List[bytes]]]:
        metadata_manager.sync_historical_watermark_tickers(table_name_watermark_postgres=self.table_watermark_name_postgres) # type: ignore
        for ticker in ticker_list:
            start_date = metadata_manager._get_smart_start_date(ticker, table_name_watermark_postgres=self.table_watermark_name_postgres) # type: ignore
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
    
    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        if not isinstance(api_data, list):
            raise ValueError(f"Dữ liệu Historical phải là List, nhưng lại nhận được: {type(api_data)}")
        processed_time = datetime.now(ZoneInfo("UTC"))
        return {
                **msg,  
                "data": api_data, 
                "message_processed_time": processed_time                   
            }
        
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

class FinancialReportsQuarterBalanceSheet(FinancialReports):
    data_type: str = "balance_sheet"
    type_id: int = 1

class FinancialReportsQuarterIncomeStatement(FinancialReports):
    data_type: str = "income_statement"
    type_id: int = 2

class FinancialReportsQuarterCashFlowDirect(FinancialReports):
    data_type: str = "cash_flow_direct"
    type_id: int = 3

class FinancialReportsQuarterCashFlowIndirect(FinancialReports):
    data_type: str = "cash_flow_indirect"
    type_id: int = 4
    

class FinancialReportsDataYear(FinancialReports):
    batch_size: int = 10
    default_limit : int = 0
    table_name_postgres: str = "ingestion_metadata_financial_reports_year"
    table_watermark_name_postgres : str = "ingestion_financial_reports_watermark_year"
    topic: str = "financial_reports_year"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/full-financial-reports?type={type_id}&year={year}&quarter=0&limit={limit}"
    target_year: int = Field(default_factory=lambda: get_target_anchor("year")[0])
    type_id : int = Field(default=0, description="""
                          1: Báo cáo cân đối kế toán(Quarterly)
                          2: Báo cáo kết quả kinh doanh(Quarterly)
                          3: Báo cáo lưu chuyển tiền tệ trực tiếp(Quarterly)
                          4: Báo cáo lưu chuyển tiền tệ gián tiếp(Quarterly)
                          """)
    pass