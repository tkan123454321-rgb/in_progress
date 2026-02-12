from sqlalchemy import *
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

load_dotenv()
# Database connection setup
def get_db_engine():
    try: 
        db_user = os.getenv('PG_USER')
        db_password = os.getenv('PG_PASSWORD')
        db_host = 'postgres'  
        db_port = '5432'       
        db_name = os.getenv('PG_DB') 
        password = quote_plus(db_password) # Encode password
        connection_str = f'postgresql://{db_user}:{password}@{db_host}:{db_port}/{db_name}' # Connection string
        engine = create_engine(connection_str) # Create engine
        return engine
    except Exception as e:
        print(f"❌ Lỗi khi khởi tạo kết nối với DB: {e}")
        return None

def get_catalog():
    catalog = load_catalog(
    "nessie_catalog",
    **{"type": "rest",
        "uri": "http://nessie:19120/iceberg",  
        "warehouse": "s3://financial-data-lake/",  
        "s3.endpoint": "http://minio:9000",  
        "s3.access-key-id": os.getenv("MINIO_USER"),  
        "s3.secret-access-key": os.getenv("MINIO_PASSWORD"),  
        "s3.region": "us-east-1",  
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
    }
)
    return catalog

