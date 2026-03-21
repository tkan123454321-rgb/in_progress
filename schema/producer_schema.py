import os

from pydantic import AliasPath, BaseModel, ConfigDict, computed_field, Field, ValidationError
from typing import Any, ClassVar, Dict, List, Tuple, TypeVar
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import uuid

class BaseMetadata(BaseModel):
    """Lớp cha để định danh mọi loại Metadata trong hệ thống"""
    model_config = ConfigDict(
        strict=True, 
        str_strip_whitespace=True, 
        extra='ignore'
    )
    
    # 1. static fields (lấy thẳng từ YAML, không động theo từng mã)
    source: str = Field(default_factory=lambda: os.getenv("MY_SOURCE", "UNKNOWN"))
    created_at_ts: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")))
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    ticker: str = "UNKNOWN"

class KafkaMetadataFundamental(BaseMetadata):

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

class KafkaMetadataHistoricalQuotes(BaseMetadata):
    
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
    
    

    
    

    