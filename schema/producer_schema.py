from pydantic import AliasPath, BaseModel, ConfigDict, computed_field, Field, ValidationError
from typing import Any, ClassVar, Dict, List, Tuple, TypeVar
from datetime import datetime
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
    table_name_postgres: str
    topic: str
    data_type: str
    source: str
    url_template: str
    # 2. dynamic fields (tự động gán khi tạo Kafka message, không cần nạp từ YAML)
    created_at_ts: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")))
    ticker: str = "UNKNOWN"
    batch_id: str = "UNKNOWN"

class KafkaMetadataFundamental(BaseMetadata):
    YAML_PATH: ClassVar[List[str]] = ["fundamental"]

    @computed_field
    def url(self) -> str:
        return self.url_template.format(ticker=self.ticker)


    def _create_kafka_message(self, ticker: str, batch_id: str) -> Tuple[str, bytes]:
        """Tạo payload và trả về (ticker, bytes)"""
        # model_copy update data động cực nhanh mà không chạy lại validation rule
        updated_instance = self.model_copy(update={"ticker": ticker, "batch_id": batch_id})
        
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
    

    