# %%
import polars as pl
from zoneinfo import ZoneInfo
from datetime import datetime
import pyarrow as pa
from vnstock import Listing
import time
import os
from utils.db_connection import get_catalog
from load.data_lake_client import LakeHouseClient
from pyiceberg.exceptions import NoSuchTableError
from IPython.display import display
from ingestion.fetch_company_list import _fetch_company_list, _fetch_upcom_company
from ingestion.ingestion_utils import _get_session
# %%

raw_dim_company = _fetch_company_list()
raw_upcom = _fetch_upcom_company()
lake_house_client = LakeHouseClient()
lake_house_client._put_raw_dim_company(raw_dim_company, name_table = "raw_companies_listing")
lake_house_client._put_raw_dim_company(raw_upcom, name_table = "raw_upcom_listing")



# %%
listing = Listing(source='VCI')
df_upcom = listing.all_symbols()
display(df_upcom)

df_group = listing.symbols_by_group("HOSE")
display(df_group)


# %%
s = _get_session()
url = "https://restv2.fireant.vn/symbols/"
response = s.get(url, timeout=10)
print(response.text)

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
