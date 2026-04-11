import psycopg
from psycopg import sql
from psycopg.rows import dict_row, DictRow
from utils.logger_config import setup_logger
import os
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Generator, Sequence

logger = setup_logger(component="utils")

class PostgresClient:
    # classvars
    _USER: ClassVar[str|None] = os.getenv("POSTGRES_USER")
    _PASS: ClassVar[str|None] = os.getenv("POSTGRES_PASSWORD")
    
        
    def _build_conn_str(self, db_name: str | None = None) -> str:
        """Hàm nội bộ để build chuỗi kết nối động"""
        return f"postgresql://{self._USER}:{self._PASS}@postgres:5432/{db_name}"

    def get_db_connection(self, db_name: str | None = None) -> psycopg.Connection:
        """
        Tạo kết nối đến Postgres.
        Trả về đối tượng connection để dùng với từ khóa 'with'.
        """
        conn_str = self._build_conn_str(db_name = db_name)
        try:
            conn = psycopg.connect(conninfo=conn_str, autocommit=True, row_factory= dict_row)  # type: ignore
            return conn
        except psycopg.Error as e:
            logger.critical(f"🔥 Không thể kết nối DB| mã lỗi: {e.sqlstate} {e}")
            raise

    def get_pgbouncer_connection(self, db_name: str | None = None) -> psycopg.Connection:
        """
        Kết nối chuyên dụng qua PgBouncer (Cổng 6432).
        Dùng riêng cho FastAPI để tối ưu connection pooling.
        """
      
        
        # Hardcode 'pgbouncer' vì mình chạy trong Docker network
        conn_str = f"postgresql://{self._USER}:{self._PASS}@pgbouncer:6432/{db_name}"
        
        try:
            # Vẫn giữ dict_row để FastAPI trả về JSON cho sướng
            conn = psycopg.connect(conninfo=conn_str, autocommit=True, row_factory=dict_row) # type: ignore
            return conn
        except psycopg.Error as e:
            logger.critical(f"🔥 Lỗi kết nối qua PgBouncer | mã lỗi: {e.sqlstate} {e}")
            raise

     
    
   
        
        
   