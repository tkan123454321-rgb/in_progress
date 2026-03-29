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
from utils.exception import MetadataManagerError


logger = setup_logger(component="utils")


class MetadataManager:
    DB_NAME : ClassVar[str] = "ops_db"
    
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        self.pg_conn : psycopg.Connection = pg_client.get_db_connection(db_name=self.DB_NAME)  # type: ignore
        self.trino_conn : trino.dbapi.Connection  = lake_client._get_trino_connection()
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





    def get_missing_tickers(self, table_name: str, tickers_set: set[str], data_type: str):
        """BƯỚC 2: Quét DB bằng Polars tốc độ cao để tìm các mã chưa được nạp"""
        query = f"SELECT ticker FROM ingestion.{table_name} where data_type = '{data_type}'"
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


    def sync_historical_watermark_tickers(self, table_name_watermark_postgres: str) -> None:
        """
        Sync the ticker list from the Gold Layer (Lakehouse) to the Postgres Watermark table.
        Ensures all active tickers are tracked before pipeline execution.
        """
        logger.info("[Watermark Sync] Reconciling ticker list from Lakehouse against Postgres...")

        # 1. Lấy danh sách Gold Tickers
        gold_tickers = self._get_ticker_list_raw(mode="other_data")

        # 2. Lấy danh sách đang có trong DB Postgres
        with self.pg_conn.cursor() as cur:
            select_query = sql.SQL("""
                SELECT ticker, ticker_status 
                FROM ingestion.{table}
            """).format(table=sql.Identifier(table_name_watermark_postgres))
            cur.execute(select_query)
            db_data = cur.fetchall()
            
            # ✅ SỬA LẠI: Dùng dict key vì đã setup dict_row
            db_tickers = {row['ticker'] for row in db_data}  # type: ignore
            inactive_db_tickers = {row['ticker'] for row in db_data if row['ticker_status'] == 'inactive'}  # type: ignore

        # 3. Tính toán độ lệch (Deltas)
        new_tickers = gold_tickers - db_tickers
        disappeared_tickers = db_tickers - gold_tickers
        reappeared_tickers = (gold_tickers & db_tickers) & inactive_db_tickers
        
        if not any([new_tickers, disappeared_tickers, reappeared_tickers]):
            logger.info("[Watermark Sync] 100% synchronized. No database updates required.")
            return
        
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as cur:
                
                # 4.1. Mã mới
                if new_tickers:
                    insert_query = sql.SQL("""
                        INSERT INTO ingestion.{table} 
                        (ticker, ticker_status, updated_at)
                        VALUES (%(ticker)s, 'active', '2018-01-01 00:00:00Z')
                    """).format(table=sql.Identifier(table_name_watermark_postgres))
                    cur.executemany(insert_query, [{'ticker': t} for t in new_tickers])
                    logger.info(f"✨ Inserted {len(new_tickers)} new tickers with baseline date 2018-01-01.")

                # 4.2. Mã biến mất
                if disappeared_tickers:
                    deactivate_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET ticker_status = 'inactive'
                        WHERE ticker = %(ticker)s
                    """).format(table=sql.Identifier(table_name_watermark_postgres))
                    cur.executemany(deactivate_query, [{'ticker': t} for t in disappeared_tickers])
                    logger.warning(f"⚠️ Deactivated {len(disappeared_tickers)} tickers missing from Lakehouse.")

                # 4.3. Mã quay lại
                if reappeared_tickers:
                    reactivate_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET ticker_status = 'active'
                        WHERE ticker = %(ticker)s
                    """).format(table=sql.Identifier(table_name_watermark_postgres))
                    cur.executemany(reactivate_query, [{'ticker': t} for t in reappeared_tickers])
                    logger.info(f"♻️ Reactivated {len(reappeared_tickers)} returning tickers.")
                
        logger.info("[Watermark Sync] Reconciliation completed successfully!")
        
    def _get_smart_start_date(self, ticker: str, table_name_watermark_postgres: str ) -> date:
        """
        Fetch the exact date to start fetching data from the watermark table.
        Note: Ticker existence is guaranteed by the prior Sync phase.
        """
        with self.pg_conn.cursor() as cursor:
            query = sql.SQL("""
                SELECT updated_at 
                FROM ingestion.{table} 
                WHERE ticker = %(ticker)s
            """).format(table=sql.Identifier(table_name_watermark_postgres))
            
            cursor.execute(query, {"ticker": ticker})
            
            # ✅ SỬA LẠI: Gọi thẳng key 'updated_at' theo chuẩn dict_row
            last_run_date = cursor.fetchone()['updated_at'].date() # type: ignore
            
            logger.debug(f"🔎 {ticker}: Fetching data from {last_run_date}")
            return last_run_date
    
    def _update_watermark(self, table_name_watermark_postgres: str, successful_tickers: set[str], batch_time: datetime) -> None:
        """
        Update the 'updated_at' column for tickers that were successfully processed.
        
        Args:
            table_name_watermark_postgres (str): The name of the watermark table.
            successful_tickers (set[str]): Set of tickers with NO errors.
            batch_time (datetime): The fixed logical time when the pipeline started.
        """
        if not successful_tickers:
            logger.warning("⚠️ No successful tickers to update. Skipping.")
            return

        logger.info(f"🔄 Saving watermark for {len(successful_tickers)} tickers at time: {batch_time.isoformat()}")

        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as pg_cur:
                # Update ONLY the tickers that are explicitly in our success list
                update_query = sql.SQL("""
                    UPDATE ingestion.{table}
                    SET updated_at = %(batch_time)s
                    WHERE ticker = %(ticker)s;
                """).format(table=sql.Identifier(table_name_watermark_postgres))

                update_payload = [
                    {"ticker": ticker, "batch_time": batch_time} 
                    for ticker in successful_tickers
                ]
                
                pg_cur.executemany(update_query, update_payload)
                
        logger.info(f"✅ Watermark updated successfully!")

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.__exit__(exc_type, exc_val, exc_tb) 
        if hasattr(self, 'trino_conn') and self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
        logger.info("🔒 Đã đóng kết nối Metadata Manager sau khi hoàn thành.")

            