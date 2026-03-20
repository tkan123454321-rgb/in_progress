from utils.logger_config import setup_logger
from utils.postgres_client import PostgresClient
from utils.lakehouse_client import LakeHouseClient, NoSuchTableError
from typing import Literal
from datetime import datetime, timedelta, timezone
import polars as pl
from polars.exceptions import PolarsError, ColumnNotFoundError, SQLInterfaceError, SQLSyntaxError
import psycopg
from psycopg import sql
from psycopg.rows import dict_row, DictRow
from typing import ClassVar, Generator, Sequence
from contextlib import contextmanager
import trino.dbapi


logger = setup_logger(component="utils")


class MetadataManager:
    DB_NAME : ClassVar[str] = "ops_db"
    
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        self.pg_conn : psycopg.Connection = pg_client.get_db_connection(db_name=self.DB_NAME)  # type: ignore
        self.trino_conn : trino.dbapi.Connection  = lake_client._get_trino_connection("read")
        self.catalog = lake_client.catalog
        self.pg_conn_str : str = pg_client._build_conn_str(db_name=self.DB_NAME)
    
    def _get_ticker_list_raw(self, mode : Literal["fundamental", "other_data"] = "fundamental" ) -> set[str]:
        try:
            if mode == "fundamental":
                table = "silver.silver_dim_company"
                tbl = self.catalog.load_table(table)
                tbl = tbl.scan(selected_fields=("ticker",)).to_polars()
                result = set(tbl["ticker"].to_list())
                logger.info(f"Successfully fetch {len(result)} tickers")
                return result
            elif mode == "other_data":
                table = "gold.gold_dim_company"
                tbl = self.catalog.load_table(table)
                tbl = tbl.scan(selected_fields=("ticker",)).to_polars()
                result = set(tbl["ticker"].to_list())
                logger.info(f"Successfully fetch {len(result)} tickers")
                return result
            else:
                raise ValueError(f"Invalid mode '{mode}' for fetching ticker list.")
        except NoSuchTableError as e:
            logger.error(f"Table {table} does not exist.")
            raise e
    
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
            self.pg_conn.execute(query, data) # type: ignore
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
            self.pg_conn.execute(cleanup_sql) # type: ignore
            logger.info(f"🧹 Đã kiểm tra và dọn dẹp bảng {table_name} (nếu dữ liệu quá 24h).")
        except psycopg.Error as e:
            logger.error(f"⚠️ Lỗi dọn dẹp DB bảng {table_name}: {e}")





    def get_missing_tickers(self, table_name, tickers_set):
        """BƯỚC 2: Quét DB bằng Polars tốc độ cao để tìm các mã chưa được nạp"""
        query = f"SELECT ticker FROM ingestion.{table_name}"
        try:
            df = pl.read_database_uri(query=query, uri=self.pg_conn_str, engine="connectorx")
            
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
    
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.__exit__(exc_type, exc_val, exc_tb) 
        if hasattr(self, 'trino_conn') and self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
        logger.info("🔒 Đã đóng kết nối Metadata Manager sau khi hoàn thành.")

            