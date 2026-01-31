from sqlalchemy import *
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

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
    

