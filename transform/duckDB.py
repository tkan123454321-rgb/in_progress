import duckdb
from dotenv import load_dotenv
import os
load_dotenv()
# 1. Kết nối (In-memory cho nhanh, không cần tạo file .db lúc này)
con = duckdb.connect()

# 2. Cài đặt Extension để nói chuyện với S3/MinIO
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL iceberg; LOAD iceberg;") # Cài luôn Iceberg cho bước sau

# 3. Khai báo thông tin đăng nhập MinIO (Ông xem lại user/pass lúc dựng docker)
con.execute(f"""
    SET s3_region='us-east-1';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id= {os.getenv('MINIO_USER')};
    SET s3_secret_access_key= {os.getenv('MINIO_PASSWORD')};
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")


# Đếm tất cả các dòng trong mọi file jsonl đang có
query = """
SELECT count(*) as total_rows
FROM read_json_auto('s3://financial-data-lake/**/*.jsonl')
"""

con.sql(query).show()