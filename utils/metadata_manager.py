from utils.logger_config import setup_logger
from utils.postgres_client import PostgresClient
from utils.lakehouse_client import LakeHouseClient, NoSuchTableError
from typing import Literal
from datetime import datetime, timedelta, timezone, date
import polars as pl
from polars.exceptions import PolarsError, ColumnNotFoundError, SQLInterfaceError, SQLSyntaxError
import psycopg
from psycopg import sql
from psycopg.rows import dict_row, DictRow
from typing import ClassVar, Generator, Sequence
from contextlib import contextmanager
import trino.dbapi
from pyiceberg.expressions import EqualTo


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
                tbl = tbl.scan(row_filter=EqualTo(term = "status", literal = "qualified"), selected_fields=("ticker",)).to_polars()
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


    def sync_historical_watermark_tickers(self) -> None:
        """
        BƯỚC 1: Đồng bộ danh sách từ Gold Layer vào bảng Watermark.
        Khởi tạo ngày mặc định (2018-01-01) cho mã mới.
        """
        logger.info("[Watermark Sync] Đang đối soát danh sách mã cổ phiếu từ Lakehouse...")

        # 1. Lấy danh sách Gold Tickers (Nguồn chân lý)
        gold_tickers = self._get_ticker_list_raw(mode="other_data")

        # 2. Lấy danh sách đang có trong DB Postgres
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT ticker, ticker_status 
                FROM ingestion.ingestion_historical_quotes_watermark
            """)
            db_data = cur.fetchall()
            db_tickers = set(row['ticker'] for row in db_data) # type: ignore
            inactive_db_tickers = set(row['ticker'] for row in db_data if row['ticker_status'] == 'inactive') # type: ignore

        new_tickers = gold_tickers - db_tickers
        disappeared_tickers = db_tickers - gold_tickers
        reappeared_tickers = (gold_tickers & db_tickers) & inactive_db_tickers

        if not any([new_tickers, disappeared_tickers, reappeared_tickers]):
            logger.info("[Watermark Sync] Danh sách đã đồng bộ 100%. Bỏ qua bước cập nhật DB.")
            return

        # 4. Thực thi vào Database (Gói gọn trong 1 Transaction)
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as cur:
                
                # 4.1. Mã mới: Insert & Gán mốc 2018-01-01
                if new_tickers:
                    insert_query = """
                        INSERT INTO ingestion.ingestion_historical_quotes_watermark 
                        (ticker, last_ingested_date, ticker_status, updated_at)
                        VALUES (%(ticker)s, '2018-01-01', 'active', CURRENT_TIMESTAMP)
                    """
                    cur.executemany(insert_query, [{'ticker': t} for t in new_tickers])
                    logger.info(f" Đã thêm {len(new_tickers)} mã mới với mốc thời gian 2018-01-01.")

                    # 4.2. Mã biến mất: Dùng %(ticker)s và truyền Dict
                if disappeared_tickers:
                    deactivate_query = """
                        UPDATE ingestion.ingestion_historical_quotes_watermark
                        SET ticker_status = 'inactive', updated_at = CURRENT_TIMESTAMP
                        WHERE ticker = %(ticker)s
                    """
                    cur.executemany(deactivate_query, [{'ticker': t} for t in disappeared_tickers])
                    logger.warning(f"   ⚠️ Đã khóa (inactive) {len(disappeared_tickers)} mã không còn trên Lakehouse.")

                # 4.3. Mã quay lại: Dùng %(ticker)s và truyền Dict
                if reappeared_tickers:
                    reactivate_query = """
                        UPDATE ingestion.ingestion_historical_quotes_watermark
                        SET ticker_status = 'active', updated_at = CURRENT_TIMESTAMP
                        WHERE ticker = %(ticker)s
                    """
                    cur.executemany(reactivate_query, [{'ticker': t} for t in reappeared_tickers])
                    logger.info(f"   ♻️ Đã mở khóa lại {len(reappeared_tickers)} mã.")
                
        logger.info("[Watermark Sync] Đối soát hoàn tất rực rỡ!")
        
        
    def _get_smart_start_date(self, ticker: str) -> date:
        """
        Lấy ngày nạp dữ liệu cuối cùng của một ticker từ bảng Watermark.
        Nếu là mã mới, trả về mốc mặc định 2018-01-01.
        """
        query = """
            SELECT last_ingested_date 
            FROM ingestion.ingestion_historical_quotes_watermark 
            WHERE ticker = %(ticker)s
        """
        cursor = self.pg_conn.execute(query, {"ticker": ticker}) # type: ignore
        result = cursor.fetchone() # type: ignore
        return result['last_ingested_date'] # type: ignore
        
        
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.__exit__(exc_type, exc_val, exc_tb) 
        if hasattr(self, 'trino_conn') and self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
        logger.info("🔒 Đã đóng kết nối Metadata Manager sau khi hoàn thành.")

            