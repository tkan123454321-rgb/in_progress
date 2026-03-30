from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DateType, TimestampType, TimestamptzType, IntegerType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, YearTransform
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder


COMMON_META_FIELDS = [
    NestedField(field_id=100, name="batch_id", field_type=StringType(), required=False),
    NestedField(field_id=101, name="data_type", field_type=StringType(), required=False),
    NestedField(field_id=102, name="source", field_type=StringType(), required=False),
    NestedField(field_id=103, name="url", field_type=StringType(), required=False),
    NestedField(field_id=104, name="created_at_ts", field_type=TimestamptzType(), required=False),
    NestedField(field_id=105, name="message_processed_time", field_type=TimestamptzType(), required=False),
    NestedField(field_id=106, name="bronze_ingested_time", field_type=TimestamptzType(), required=False) # ID = 106
]
ORIGINAL_TICKER_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=False),
    NestedField(field_id=2, name="organ_name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="icb_name3", field_type=StringType(), required=False),
    NestedField(field_id=4, name="icb_name2", field_type=StringType(), required=False),
    NestedField(field_id=5, name="icb_name4", field_type=StringType(), required=False),
    NestedField(field_id=6, name="com_type_code", field_type=StringType(), required=False),
    NestedField(field_id=7, name="icb_code1", field_type=StringType(), required=False),
    NestedField(field_id=8, name="icb_code2", field_type=StringType(), required=False),
    NestedField(field_id=9, name="icb_code3", field_type=StringType(), required=False),
    NestedField(field_id=10, name="icb_code4", field_type=StringType(), required=False),
    NestedField(field_id=106, name="bronze_ingested_time", field_type=TimestamptzType(), required=False)
)
# =======================================================================
# 2. ĐỊNH NGHĨA SCHEMA (Dùng dấu * để giải nén mảng dùng chung vào)
# =======================================================================
HISTORICAL_QUOTES_SCHEMA = Schema(
    NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
    NestedField(field_id=2, name="event_date", field_type=DateType(), required=False),
    NestedField(field_id=3, name="data", field_type=StringType(), required=False),
    *COMMON_META_FIELDS  # Tự động nhét 7 cột hệ thống vào đây
)

FUNDAMENTAL_SCHEMA = Schema(
    NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    *COMMON_META_FIELDS  # Tự động nhét 7 cột hệ thống vào đây
)

FINANCIAL_REPORTS_SCHEMA = Schema(
    NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
    NestedField(field_id=2, name="year", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="quarter", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="indicator_id", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="indicator_name", field_type=StringType(), required=False),
    NestedField(field_id=6, name="value", field_type=DoubleType(), required=False), 
    *COMMON_META_FIELDS  # Tự động nhét 7 cột hệ thống vào đây
)

# =======================================================================
# 3. ĐỊNH NGHĨA PARTITION & SORT (Gộp làm 1 duy nhất)
# =======================================================================

PARTITION_BY_EVENT_YEAR = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=YearTransform(), name="event_year")
)
PARTITION_BY_REPORT_YEAR = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="report_year")
)
FINANCIAL_REPORTS_PARTITION = PartitionSpec(
    PartitionField(
        source_id=2,   # ID của cột 'year'
        field_id=1000, 
        transform=IdentityTransform(), 
        name="year"
    ),
    PartitionField(
        source_id=101, 
        field_id=1001, 
        transform=IdentityTransform(), 
        name="data_type"
    )
)

UNPARTITIONED = PartitionSpec()

# 🔥 DUY NHẤT 1 LUẬT SORT DÙNG CHO CẢ LÀNG (Trỏ vào ID 106)
GLOBAL_SORT_BY_BRONZE_TIME = SortOrder(
    SortField(
        source_id=106, # Trỏ thẳng vào bronze_ingested_time
        transform=IdentityTransform(), 
        direction=SortDirection.DESC, 
        null_order=NullOrder.NULLS_LAST
    )
)

# =======================================================================
# 4. SỔ ĐĂNG KÝ (REGISTRY)
# =======================================================================

TABLE_REGISTRY = {
    "original_ticker_list": {
        "schema": ORIGINAL_TICKER_SCHEMA,
        "partition_spec": UNPARTITIONED,
        "sort_order": None
    },
    "historical_quotes": {
        "schema": HISTORICAL_QUOTES_SCHEMA,
        "partition_spec": PARTITION_BY_EVENT_YEAR,
        "sort_order": GLOBAL_SORT_BY_BRONZE_TIME
    },
    "fundamental_1": {
        "schema": FUNDAMENTAL_SCHEMA,
        "partition_spec": UNPARTITIONED,
        "sort_order": GLOBAL_SORT_BY_BRONZE_TIME
    },
    "fundamental_2": {
        "schema": FUNDAMENTAL_SCHEMA,
        "partition_spec": UNPARTITIONED,
        "sort_order": GLOBAL_SORT_BY_BRONZE_TIME
    },
    "financial_reports_quarter": {
        "schema": FINANCIAL_REPORTS_SCHEMA,
        "partition_spec": FINANCIAL_REPORTS_PARTITION, # 💡 Thay bằng spec mới
        "sort_order": GLOBAL_SORT_BY_BRONZE_TIME
    },
    "financial_reports_year": {
        "schema": FINANCIAL_REPORTS_SCHEMA,
        "partition_spec": FINANCIAL_REPORTS_PARTITION, # 💡 Thay bằng spec mới
        "sort_order": GLOBAL_SORT_BY_BRONZE_TIME
    }
}