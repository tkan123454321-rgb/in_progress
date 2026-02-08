# %%
import polars as pl
import pyarrow as pa
from vnstock import Listing
from datetime import datetime
import os
from utils.db_connection import get_catalog
# %%
iceberg_catalog = get_catalog()
iceberg_catalog.create_namespace_if_not_exists("bronze")













# %%




# --- 1. CONFIGURATION (Kết nối Nessie & MinIO) ---
# Đây là phần quan trọng nhất để PyIceberg tìm thấy "nhà"


# --- 2. EXTRACT DATA (Lấy dữ liệu thô) ---
print("🚀 Đang lấy dữ liệu VNStock...")
# Lấy list chứng khoán (demo)
df_hose = pl.from_pandas(listing.symbols_by_group('HOSE'))
df_hnx = pl.from_pandas(listing.symbols_by_group('HNX'))
df_raw = pl.concat([df_hose, df_hnx])

# Thêm cột thời gian ingest để theo dõi
df_raw = df_raw.with_columns(
    pl.lit(datetime.now()).alias("ingestion_time")
)

# [QUAN TRỌNG] Chuyển đổi Polars -> PyArrow Table
# PyIceberg làm việc trực tiếp với PyArrow Table
arrow_table = df_raw.to_arrow()

# --- 3. WRITE TO LAKEHOUSE (Ghi thẳng vào MinIO) ---
NAMESPACE = "finance_bronze" # Schema trong Trino
TABLE_NAME = "raw_listing"

print(f"📦 Đang đẩy {len(arrow_table)} dòng vào bảng {NAMESPACE}.{TABLE_NAME}...")

try:
    # Cố gắng load bảng, nếu chưa có thì tạo mới
    try:
        table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")
        print(" -> Bảng đã tồn tại. Đang ghi đè (Overwrite)...")
        
        # overwrite: Xóa dữ liệu cũ, thay bằng mới (Snapshot mới)
        # append: Nối thêm đuôi
        table.overwrite(arrow_table) 
        
    except Exception:
        print(" -> Bảng chưa có. Đang tạo mới (Create)...")
        # Tạo bảng dựa trên Schema của PyArrow (Tự động map kiểu dữ liệu)
        table = catalog.create_table(
            identifier=f"{NAMESPACE}.{TABLE_NAME}",
            schema=arrow_table.schema,
            location=f"s3://lakehouse/{NAMESPACE}/{TABLE_NAME}" # Đường dẫn trong MinIO bucket
        )
        table.append(arrow_table)

    print("✅ Xong! Dữ liệu đã nằm an toàn trong MinIO dưới dạng Parquet.")

except Exception as e:
    print(f"❌ Lỗi rồi đại vương ơi: {e}")
# %%
