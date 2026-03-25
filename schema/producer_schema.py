from abc import ABC, abstractmethod
import json
import os
import polars as pl
from pydantic import AliasPath, BaseModel, ConfigDict, computed_field, Field, ValidationError
from typing import Any, ClassVar, Dict, List, Tuple, TypeVar
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
logger = setup_logger(component="schema")
class BaseMetadata(BaseModel,ABC):
    """Lớp cha để định danh mọi loại Metadata trong hệ thống"""
    model_config = ConfigDict(
        strict=True, 
        str_strip_whitespace=True, 
        extra='ignore'
    )
    batch_size: int
    table_name_postgres: str 
    topic: str
    data_type: str
    url_template: str
    
    # 1. static fields (lấy thẳng từ YAML, không động theo từng mã)
    source: str = Field(default_factory=lambda: os.getenv("MY_SOURCE", "UNKNOWN"))
    created_at_ts: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")))
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    ticker: str = "UNKNOWN"
    
    def __enter__(self):
        """Khởi tạo tiêu chuẩn cho mọi class con"""
        logger.info(f"🚪 [BASE] Bắt đầu phiên làm việc với topic: {self.topic}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Đóng phiên làm việc tiêu chuẩn"""
        if exc_type:
            logger.warning(f"⚠️ [BASE] Phiên làm việc [{self.topic}] bị ngắt do lỗi: {exc_type.__name__}.")
        else:
            logger.info(f"✅ [BASE] Phiên làm việc [{self.topic}] hoàn tất êm đẹp.")
        
        # Luôn return False để Python không "nuốt" mất lỗi (nếu có)
        return False
    
    @property
    def iceberg_schema(self) -> Schema:
        """Tự động tra sổ lấy Schema dựa vào data_type của Class con"""
        if self.data_type not in TABLE_REGISTRY:
            raise ValueError(f"Chưa định nghĩa Schema cho loại dữ liệu: {self.data_type}")
        return TABLE_REGISTRY[self.data_type]["schema"]

    @property
    def iceberg_partition_spec(self) -> PartitionSpec:
        """Tự động tra sổ lấy Partition Spec dựa vào data_type của Class con"""
        if self.data_type not in TABLE_REGISTRY:
            raise ValueError(f"Chưa định nghĩa Partition cho loại dữ liệu: {self.data_type}")
        return TABLE_REGISTRY[self.data_type]["partition_spec"]
    
    @computed_field
    @abstractmethod
    def url(self) -> str:
        """Bắt buộc class con phải định nghĩa cách tạo URL"""
        pass
    
    @abstractmethod
    def _create_kafka_message(self, ticker: str, **kwargs) -> Tuple[str, bytes]:
        """
        Class con tự quyết định nhận thêm tham số gì (offset, start_date...)
        Nhưng bắt buộc phải trả về Tuple[str, bytes]
        """
        pass

    @abstractmethod
    def transform_message(self, msg: dict[str, Any], api_data: Any) -> dict:
        """Bắt buộc phải có hàm xử lý dữ liệu trả về từ API"""
        pass
    @abstractmethod
    def _build_arrow_payload_lazy(self, buffer: list[dict[str, Any]]) -> pa.Table:
        """Bắt buộc phải có hàm chuyển đổi list dict thành PyArrow Table"""
        pass
    
    
    
class KafkaMetadataFundamental(BaseMetadata):
    batch_size : int = 100
    table_name_postgres: str = "ingestion_metadata_fundamental"
    topic: str = "fundamental"
    data_type: str = "fundamental"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/fundamental"

    @computed_field
    def url(self) -> str:
        return self.url_template.format(ticker=self.ticker)


    def _create_kafka_message(self, ticker: str) -> Tuple[str, bytes]:
        """Tạo payload và trả về (ticker, bytes)"""
        # model_copy update data động cực nhanh mà không chạy lại validation rule
        updated_instance = self.model_copy(update={"ticker": ticker})
        
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


class KafkaMetadataHistoricalQuotes(BaseMetadata):
    batch_size: int = 10
    default_limit: int = 500
    default_offset: int = 0
    table_name_postgres: str = "ingestion_metadata_historical_quotes"
    topic: str = "historical_quotes"
    data_type: str = "historical_quotes"
    url_template: str = "https://restv2.{source}.vn/symbols/{ticker}/historical-quotes?startDate={start_date}&endDate={end_date}&offset={offset}&limit={limit}"
    start_date: date = Field(default=date(2018, 1, 1))
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

    def _create_kafka_message(self, ticker: str, start_date: date, offset: int) -> Tuple[str, bytes]:
        
        updated_instance = self.model_copy(update={
            "ticker": ticker, 
            "start_date": start_date,
            "default_offset": offset # Ghi đè cái 2018 bằng ngày thực tế từ Postgres
            # end_date không cần update vì mặc định nó đã là hôm qua rồi!
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1. Gọi __exit__ của lớp cha để tận dụng cái log Cảnh báo/Thành công tiêu chuẩn
        super().__exit__(exc_type, exc_val, exc_tb)

        # 2. Bắt đầu xử lý logic đặc thù của riêng Historical Quotes
        logger.info("🔄 [HISTORICAL] Bắt đầu chốt sổ Watermark cho Historical Quotes...")
        try:
            with MetadataManager(pg_client=PostgresClient(), lake_client=LakeHouseClient()) as metadata_manager:
                metadata_manager._update_max_ingested_date()
        except Exception as e:
            logger.error(f"❌ [HISTORICAL] Lỗi khi cố gắng cập nhật Watermark lúc thoát: {e}")
            raise e
  


    
    

    
    

    