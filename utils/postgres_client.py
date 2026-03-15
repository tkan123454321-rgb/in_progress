import psycopg
from psycopg import sql
from utils.logger_config import setup_logger
import os
from datetime import datetime
import polars as pl
from polars.exceptions import PolarsError, ColumnNotFoundError, SQLInterfaceError, SQLSyntaxError
from typing import ClassVar, Generator, Sequence

logger = setup_logger(component="utils")

class PostgresClient:
    # classvars
    _USER: ClassVar[str|None] = os.getenv("POSTGRES_USER")
    _PASS: ClassVar[str|None] = os.getenv("POSTGRES_PASSWORD")
    
    def __init__(self, conn: psycopg.Connection):
        """
        Bơm thẳng connection đã mở và config vào đây.
        Mọi method bên dưới sẽ dùng chung self.conn và self.config này.
        """
        self.conn = conn
        self.db = conn.info.dbname
        self.conn_str = self._build_conn_str(self.db)
        
    @classmethod
    def _build_conn_str(cls, db_name: str) -> str:
        """Hàm nội bộ để build chuỗi kết nối động"""
        return f"postgresql://{cls._USER}:{cls._PASS}@postgres:5432/{db_name}"
    
    @classmethod
    def get_db_connection(cls, db_name: str) -> psycopg.Connection:
        """
        Tạo kết nối đến Postgres.
        Trả về đối tượng connection để dùng với từ khóa 'with'.
        """
        conn_str = cls._build_conn_str(db_name)
        try:
            conn = psycopg.connect(conninfo=conn_str, autocommit=True)
            return conn
        except psycopg.Error as e:
            logger.critical(f"🔥 Không thể kết nối DB| mã lỗi: {e.sqlstate} {e}")
            raise



    def log_metadata_to_db(self, table_name: str, batch_id: str, topic: str, ticker: str, data_type: str):
        query = sql.SQL("""
            INSERT INTO ingestion.{table_name} 
            (batch_id, topic_name, data_type, ticker, created_time)
            VALUES (%(batch_id)s, %(topic)s, %(data_type)s, %(ticker)s, %(created_time)s)
        """).format(table_name=sql.Identifier(table_name))
        
        data = {
            "batch_id": batch_id,
            "topic": topic,
            "data_type": data_type,
            "ticker": ticker,
            "created_time": datetime.now()
        }
        
        try:
            self.conn.execute(query, data) # type: ignore
        except psycopg.Error as e:
            # Bắt ĐÍCH DANH lỗi của psycopg (Database Error)
            # Báo warning để vòng lặp đi tiếp, không làm chết luồng chạy của Kafka
            logger.error(f"⚠️ Lỗi Database khi ghi log cho mã {ticker}: {e.sqlstate}, {e}")
            raise



            
    def cleanup_state_table(self, table_name):
        """BƯỚC 1: Chạy 1 lần ở đầu pipeline - Xóa sạch bảng nếu dữ liệu cũ hơn 24h"""
        cleanup_sql = sql.SQL("""
            DO $$
            BEGIN
                IF (SELECT MAX(created_time) FROM ingestion.{table_name}) < NOW() - INTERVAL '24 HOURS' THEN
                    TRUNCATE TABLE ingestion.{table_name};
                END IF;
            END $$;
        """).format(table_name=sql.Identifier(table_name))
        try:
            self.conn.execute(cleanup_sql) # type: ignore
            logger.info(f"🧹 Đã kiểm tra và dọn dẹp bảng {table_name} (nếu dữ liệu quá 24h).")
        except psycopg.Error as e:
            logger.error(f"⚠️ Lỗi dọn dẹp DB bảng {table_name}: {e}")





    def get_missing_tickers(self, table_name, tickers_set):
        """BƯỚC 2: Quét DB bằng Polars tốc độ cao để tìm các mã chưa được nạp"""
        query = f"SELECT ticker FROM ingestion.{table_name}"
        try:
            df = pl.read_database_uri(query=query, uri=self.conn_str, engine="connectorx")
            
            if df.is_empty():
                processed_tickers_set = set()
            else:
                processed_tickers_set = set(df["ticker"].to_list())
                
            missing_tickers = tickers_set - processed_tickers_set
            
            if missing_tickers:
                logger.warning(f"⚠️ Có {len(missing_tickers)} mã chưa được nạp trong 24h qua. Bắt đầu chạy bù.")
            else:
                logger.info("✅ Tất cả mã đã được nạp đầy đủ trong 24h qua.")
                
            return missing_tickers

        # 1. BẮT LỖI CẤU TRÚC BẢNG (FATAL)
        except ColumnNotFoundError as e:
            logger.error(f"FATAL SCHEMA: Bảng {table_name} bị thiếu cột 'ticker'. Chi tiết: {e}")
            raise  # Ném lỗi ra ngoài, bắt buộc sập luồng chính!

        # 2. BẮT LỖI CÚ PHÁP SQL (FATAL)
        except (SQLInterfaceError, SQLSyntaxError) as e:
            logger.error(f"FATAL QUERY: Sai cú pháp SQL khi gọi Polars. Chi tiết: {e}")
            raise  # Ném lỗi ra ngoài, bắt buộc sập luồng chính!
            
        except PolarsError as e:
            logger.critical(f"FATAL POLARS: Lỗi xử lý dữ liệu nội bộ của Polars: {e}")
            raise  
    
    def prepare_temp_minio_table(self) -> None:
        """BƯỚC 1: Dọn dẹp và tạo bảng Tmp. Không cần cursor nữa."""
        setup_sql = """
            DROP TABLE IF EXISTS nessie_gc.temp_minio_object_locations;
            CREATE TABLE nessie_gc.temp_minio_object_locations (
                location VARCHAR PRIMARY KEY,
                scanned_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """
        # Quất thẳng execute từ connection
        self.conn.execute(setup_sql)
        logger.info("Đã tạo bảng tạm: temp_minio_object_locations.")

    def insert_minio_location_batch(self, batch: Sequence[tuple]) -> None:
        """BƯỚC 2: Nạp 1 batch dữ liệu vào bảng Tmp."""
        insert_sql = "INSERT INTO nessie_gc.temp_minio_object_locations (location) VALUES (%s)"
        # executemany cũng gọi thẳng từ connection luôn
        with self.conn.cursor() as cur:
            cur.executemany(insert_sql, batch)

    def swap_minio_location_tables(self) -> None:
        """BƯỚC 3: Tráo bảng Tmp thành bảng Chính."""
        swap_sql = """
            DROP TABLE IF EXISTS nessie_gc.minio_object_locations CASCADE;
            ALTER TABLE nessie_gc.temp_minio_object_locations RENAME TO minio_object_locations;
        """
        self.conn.execute(swap_sql)
        logger.info("🔄 Đã Swap thành công sang bảng chính: minio_object_locations.")
    
    def yield_orphan_location_batches(self) -> Generator[str, None, None]:
        """
        [PHASE 2] Generator: Dùng EXCEPT tìm rác và nhả về từng mẻ (chunk).
        """
        set_ram_sql = "SELECT set_config('work_mem', '50MB', true);"
        query = sql.SQL( """
            SELECT location 
            FROM nessie_gc.minio_object_locations
            EXCEPT
            SELECT REPLACE(base_location, 's3://financial-data-lake/', '')
            FROM nessie_gc.gc_live_set_content_locations;
        """)
        
        with self.conn.transaction():
            self.conn.execute(set_ram_sql)
            with self.conn.cursor(name="orphan_stream_cursor") as cur:
                # Ép RAM cho query
                # Thực thi EXCEPT
                cur.execute(query)
                
                # Bơm nước trả về từng mẻ
                while True:
                    batch = cur.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        yield row[0]
   