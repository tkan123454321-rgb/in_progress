from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DateType, TimestampType, TimestamptzType, IntegerType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform


HISTORICAL_QUOTES_SCHEMA = Schema(
    # --- Các cột Khóa chính (Bắt buộc phải có) ---
    NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
    NestedField(field_id=2, name="event_date", field_type=DateType(), required=False),
    NestedField(field_id=3, name="data", field_type=StringType(), required=False),
    
    # --- Các cột Metadata từ Kafka ---
    NestedField(field_id=4, name="batch_id", field_type=StringType(), required=False),
    NestedField(field_id=5, name="data_type", field_type=StringType(), required=False),
    NestedField(field_id=6, name="source", field_type=StringType(), required=False),
    NestedField(field_id=7, name="url", field_type=StringType(), required=False),
    
    # --- Các cột Thời gian hệ thống ---
    NestedField(field_id=8, name="created_at_ts", field_type=TimestamptzType(), required=False),
    NestedField(field_id=9, name="message_processed_time", field_type=TimestamptzType(), required=False),
    NestedField(field_id=10, name="bronze_ingested_time", field_type=TimestamptzType(), required=False)
)

FUNDAMENTAL_SCHEMA = Schema(
    # --- Cột Khóa chính ---
    NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    NestedField(field_id=3, name="batch_id", field_type=StringType(), required=False),
    NestedField(field_id=4, name="data_type", field_type=StringType(), required=False),
    NestedField(field_id=5, name="source", field_type=StringType(), required=False),
    NestedField(field_id=6, name="url", field_type=StringType(), required=False),
    NestedField(field_id=7, name="created_at_ts", field_type=TimestamptzType(), required=False),
    NestedField(field_id=8, name="message_processed_time", field_type=TimestamptzType(), required=False),
    NestedField(field_id=9, name="bronze_ingested_time", field_type=TimestamptzType(), required=False)
)

FINANCIAL_REPORTS_SCHEMA = Schema(
    # --- Các cột Dữ liệu Báo cáo tài chính (Core Data) ---
    NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
    NestedField(field_id=2, name="year", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="quarter", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="indicator_id", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="indicator_name", field_type=StringType(), required=False),
    NestedField(field_id=6, name="value", field_type=DoubleType(), required=False), # Chữ Float64 trong Polars map đúng bằng DoubleType ở đây
    
    # --- Các cột Metadata từ Kafka ---
    NestedField(field_id=7, name="batch_id", field_type=StringType(), required=False),
    NestedField(field_id=8, name="data_type", field_type=StringType(), required=False),
    NestedField(field_id=9, name="source", field_type=StringType(), required=False),
    NestedField(field_id=10, name="url", field_type=StringType(), required=False),
    
    # --- Các cột Thời gian hệ thống ---
    NestedField(field_id=11, name="created_at_ts", field_type=TimestamptzType(), required=False),
    NestedField(field_id=12, name="message_processed_time", field_type=TimestamptzType(), required=False),
    NestedField(field_id=13, name="bronze_ingested_time", field_type=TimestamptzType(), required=False)
)

TICKER_PARTITION = PartitionSpec(
    PartitionField(
        source_id=1,   # Trỏ vào cột ticker (field_id = 1)
        field_id=1000, 
        transform=IdentityTransform(), 
        name="ticker"
    )
)

UNPARTITIONED = PartitionSpec()

# =======================================================================
# 3. SỔ ĐĂNG KÝ (REGISTRY) - Để gọi ra dùng cho tiện
# =======================================================================

TABLE_REGISTRY = {
    "historical_quotes": {
        "schema": HISTORICAL_QUOTES_SCHEMA,
        "partition_spec": TICKER_PARTITION # Bảng giá lịch sử nặng nên chia theo mã
    },
    "fundamental": {
        "schema": FUNDAMENTAL_SCHEMA,
        "partition_spec": UNPARTITIONED    # Bảng cơ bản nhẹ hều thì gom chung 1 cục
    },
    "financial_reports": {
        "schema": FINANCIAL_REPORTS_SCHEMA,
        "partition_spec": TICKER_PARTITION # Báo cáo tài chính cũng khá nặng, chia theo mã cho dễ quản lý
    }
    
}