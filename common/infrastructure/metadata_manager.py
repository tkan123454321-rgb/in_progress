from __future__ import annotations
from common.core.logger_config import setup_logger
from common.clients.postgres_client import PostgresClient
from common.clients.lakehouse_client import LakeHouseClient, NoSuchTableError
from typing import Literal, TYPE_CHECKING
from datetime import datetime, timedelta, timezone, date
import polars as pl
import psycopg
from psycopg import sql
from psycopg.rows import dict_row, DictRow
from typing import ClassVar, Generator, Sequence
from contextlib import contextmanager
import trino.dbapi
from pyiceberg.expressions import EqualTo
from common.core.exception import MetadataManagerError
from common.core.time_utils import get_target_anchor, get_fallback_year
from zoneinfo import ZoneInfo
if TYPE_CHECKING:
    from schema.producer_schema import BaseMetadata


logger = setup_logger(component="utils")


class MetadataManager:
    DB_NAME : ClassVar[str] = "ops_db"
    
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        self.pg_conn : psycopg.Connection = pg_client.get_db_connection(db_name=self.DB_NAME)  # type: ignore
        self.trino_conn : trino.dbapi.Connection  = lake_client._get_trino_connection()
        self.catalog = lake_client.catalog
        self.pg_conn_str : str = pg_client._build_conn_str(db_name=self.DB_NAME)
    
    def _get_ticker_list_raw(self, mode : Literal["fundamental", "other_data", "vnindex"] = "fundamental" ) -> set[str]:
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
            elif mode == "vnindex":
                return {'VNINDEX'}
            else:
                raise ValueError(f"Invalid mode '{mode}' for fetching ticker list.")
        except NoSuchTableError as e:
            logger.error(f"Table {table} does not exist.")
            raise e
    
    def log_metadata_to_db(self, table_name: str, batch_id: str, ticker: str, data_type: str):
        query = sql.SQL("""
            INSERT INTO ingestion.{table_name} 
            (batch_id, data_type, ticker, created_time)
            VALUES (%(batch_id)s, %(data_type)s, %(ticker)s, %(created_time)s)
        """).format(table_name=sql.Identifier(table_name))
        
        data = {
            "batch_id": batch_id,
            "data_type": data_type,
            "ticker": ticker,
            "created_time": datetime.now()
        }
        
        try:
            self.pg_conn.execute(query, data) 
            logger.debug(f"Logged metadata for ticker '{ticker}' with batch_id '{batch_id}' and data_type '{data_type}' into Postgres.")
        except psycopg.Error as e:
            logger.error(f"⚠️ Lỗi Database khi ghi log cho mã {ticker}: {e.sqlstate}, {e}")
            raise



            
    def cleanup_kafka_state(self, table_name: str, data_type: str) -> None:
        """
        Runs once at the start of the pipeline for each data type.
        Purges Kafka state records SPECIFIC TO THAT DATA TYPE if they are older than 24 hours.
        """
        cleanup_sql = sql.SQL("""
            DELETE FROM ingestion.{table}
            WHERE data_type = %(data_type)s 
              AND created_time < NOW() - INTERVAL '24 HOURS';
        """).format(table=sql.Identifier(table_name))
        

        with self.pg_conn.cursor() as cur:
            cur.execute(cleanup_sql, {'data_type': data_type})
            deleted_rows = cur.rowcount 
            
        # Log kết quả ra màn hình
        if deleted_rows > 0:
            logger.info(f"🧹 [KAFKA_STATE] Purged {deleted_rows} expired records (>24h) for '{data_type}'.")
        else:
            logger.info(f"✨ [KAFKA_STATE] No expired records found for '{data_type}'. State is clean.")
                
        



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


    def sync_watermark(self, config: BaseMetadata) -> None:
        """
        Sync the ticker list from the Gold Layer (Lakehouse) to the Postgres Watermark table.
        Ensures all active tickers are tracked before pipeline execution.
        """
        logger.info("[Watermark Sync] Reconciling ticker list from Lakehouse against Postgres...")
        if not config.table_watermark_name_postgres:
            raise ValueError(f"⚠️ [{config.data_type}] Thiếu tên bảng Watermark Postgres!")
        # 1. Lấy danh sách Gold Tickers
        gold_tickers = self._get_ticker_list_raw(mode= config.ticker_list_mode)

        # 2. Lấy danh sách đang có trong DB Postgres
        with self.pg_conn.cursor() as cur:
            select_query = sql.SQL("""
                SELECT ticker, ticker_status 
                FROM ingestion.{table}
                WHERE data_type = %(data_type)s
            """).format(table=sql.Identifier(config.table_watermark_name_postgres))
            
            cur.execute(select_query, {'data_type': config.data_type})
            db_data = cur.fetchall()
            
            # ✅ SỬA LẠI: Dùng dict key vì đã setup dict_row
            db_tickers = {row['ticker'] for row in db_data}  # type: ignore
            inactive_db_tickers = {row['ticker'] for row in db_data if row['ticker_status'] == 'inactive'}  # type: ignore

        # 3. Tính toán độ lệch (Deltas)
        new_tickers = gold_tickers - db_tickers
        disappeared_tickers = db_tickers - gold_tickers
        reappeared_tickers = (gold_tickers & db_tickers) & inactive_db_tickers
        
        if not any([new_tickers, disappeared_tickers, reappeared_tickers]):
            logger.info("[Watermark Sync] 100%. synchronized. No database updates required.")
            return
        
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as cur:
                
                # 4.1. Mã mới
                if new_tickers:
                    fallback_year = get_fallback_year()
                    fallback_time_str = f"{fallback_year}-01-01 00:00:00+00"
                    insert_query = sql.SQL("""
                        INSERT INTO ingestion.{table} 
                        (ticker, data_type, ticker_status, updated_at)
                        VALUES (%(ticker)s, %(data_type)s, 'active', %(fallback_time)s)
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    cur.executemany(insert_query, [{'ticker': t, 'data_type': config.data_type, 'fallback_time': fallback_time_str} for t in new_tickers])
                    logger.info(f"✨ Inserted {len(new_tickers)} new tickers for {config.data_type}.")

                # 4.2. Mã biến mất
                if disappeared_tickers:
                    deactivate_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET ticker_status = 'inactive'
                        WHERE ticker = %(ticker)s AND data_type = %(data_type)s
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    cur.executemany(deactivate_query, [{'ticker': t, 'data_type': config.data_type} for t in disappeared_tickers])
                    logger.warning(f"⚠️ Deactivated {len(disappeared_tickers)} tickers.")

                # 4.3. Mã quay lại
                if reappeared_tickers:
                    reactivate_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET ticker_status = 'active'
                        WHERE ticker = %(ticker)s AND data_type = %(data_type)s
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    cur.executemany(reactivate_query, [{'ticker': t, 'data_type': config.data_type} for t in reappeared_tickers])
                    logger.info(f"♻️ Reactivated {len(reappeared_tickers)} returning tickers.")
                
        logger.info("[Watermark Sync] Reconciliation completed successfully!")
        
    def _get_smart_start_date(self, ticker: str, config: BaseMetadata) -> date:
        """
        Fetch the exact date to start fetching data from the unified watermark table.
        """
        if not config.table_watermark_name_postgres:
            raise ValueError(f"⚠️ [{config.data_type}] Thiếu tên bảng Watermark Postgres!")
        with self.pg_conn.cursor() as cursor:
            query = sql.SQL("""
                SELECT updated_at 
                FROM ingestion.{table} 
                WHERE ticker = %(ticker)s AND data_type = %(data_type)s
            """).format(table=sql.Identifier(config.table_watermark_name_postgres))
            
            cursor.execute(query, {"ticker": ticker, "data_type": config.data_type})
            
            last_run_date = cursor.fetchone()['updated_at'].date() # type: ignore
            
            logger.debug(f"🔎 {ticker} [{config.data_type}]: Fetching data from {last_run_date}")
            return last_run_date
    
    def reconcile_watermark_from_lakehouse(self, config: BaseMetadata) -> None:
        """
        Quét Iceberg 2 năm tài chính gần nhất.
        - Nếu trống rỗng -> Reset toàn bộ (Truncate Iceberg + Xóa DB).
        - Nếu có data -> Lấy MAX(bronze_ingested_time) cập nhật vào Postgres.
        """
        if not config.table_watermark_name_postgres:
            raise ValueError(f"⚠️ [{config.data_type}] Thiếu tên bảng Watermark Postgres!")
        
        logger.info(f"🔍 [LAKEHOUSE SYNC] Bắt đầu quét dữ liệu 2 năm gần nhất cho {config.data_type}...")
        check_table_query = f"""
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = '{config.bronze_layer_name}'
        """
        with self.trino_conn.cursor() as cursor:
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone() 
            
        # Nếu bảng vừa bị Drop tay, gọi hàm dọn dẹp Postgres là xong
        if not table_exists:
            logger.warning(f"⚠️ Bảng bronze.{config.bronze_layer_name} CHƯA TỒN TẠI, bắt đầu hard reset hệ thống.")
            self._reset_postgres_state(config)
            return
        
        current_year = datetime.now(ZoneInfo("UTC")).year
        lookback_year = current_year - 3
        
        # 1. FAST PATH: Quét ngọn Iceberg, chốt ngày Ingest lớn nhất
        query = f"""
            SELECT ticker, MAX(bronze_ingested_time) as last_ingested_time
            FROM bronze.{config.bronze_layer_name}
            WHERE year >= {lookback_year} AND data_type = '{config.data_type}'
            GROUP BY ticker
        """
        
        with self.trino_conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        if not results:
            logger.warning(f"⚠️ Không tìm thấy dữ liệu nào trong Iceberg cho loại dữ liệu: {config.data_type}. Thực hiện reset toàn bộ trạng thái...")
            # 2. Xóa sạch Watermark cũ trong Postgres của data_type này
            with self.pg_conn.transaction():
                with self.pg_conn.cursor() as pg_cur:
                    delete_pg_query = sql.SQL(
                        "DELETE FROM ingestion.{table} WHERE data_type = %(data_type)s"
                    ).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    pg_cur.execute(delete_pg_query, {"data_type": config.data_type})
                    
            logger.info(f"✅ DOOMSDAY {config.data_type} HOÀN TẤT.")
            with self.trino_conn.cursor() as trino_cur:
                delete_iceberg_query = f"""
                    DELETE FROM bronze.{config.bronze_layer_name} 
                    WHERE data_type = '{config.data_type}'
                """
                trino_cur.execute(delete_iceberg_query)
            logger.info(f"✅ Đã dọn sạch dữ liệu cũ của '{config.data_type}' trong Iceberg.")
            
            return
        else:
            logger.info(f"✅ Tìm thấy dữ liệu trong Iceberg cho '{config.data_type}'. Bắt đầu cập nhật watermark mới nhất trong Postgres...")
            with self.pg_conn.transaction():
                with self.pg_conn.cursor() as pg_cur:
                    update_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET updated_at = %(last_ingested_time)s
                        WHERE ticker = %(ticker)s AND data_type = %(data_type)s
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    payload = [
                            {
                                "ticker": row[0],
                                "last_ingested_time": row[1],
                                "data_type": config.data_type
                            }
                            for row in results
                        ]
                    
                    # executemany sẽ tự động loop qua payload và thực thi an toàn
                    pg_cur.executemany(update_query, payload)
            logger.info(f"✅ Đồng bộ bảng watermark thành công cho loại dữ liệu:'{config.data_type}' hoàn tất cho {len(payload)} mã!")
            
    def _reset_postgres_state(self, config: BaseMetadata) -> None:
        """
        Hàm nội bộ: Dọn dẹp sạch sẽ toàn bộ trạng thái kéo API và Kafka 
        của một data_type cụ thể trong Postgres.
        """
        logger.info(f"🧹 Bắt đầu dọn dẹp toàn bộ trạng thái cũ của '{config.data_type}' trong Postgres...")
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as pg_cur:
                if config.table_watermark_name_postgres:
                # 1. Xóa trạng thái trong bảng Watermark chính
                    cleanup_watermark_sql = sql.SQL("""
                        DELETE FROM ingestion.{table}
                        WHERE data_type = %(data_type)s;
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    pg_cur.execute(cleanup_watermark_sql, {"data_type": config.data_type})
                    
                # 2. Xóa trạng thái trong bảng Kafka Metadata (nếu có)
                if config.table_name_postgres:
                    cleanup_kafka_sql = sql.SQL("""
                        DELETE FROM ingestion.{table}
                        WHERE data_type = %(data_type)s;
                    """).format(table=sql.Identifier(config.table_name_postgres))
                    pg_cur.execute(cleanup_kafka_sql, {"data_type": config.data_type})
        
        logger.info(f"✅ Đã reset toàn bộ trạng thái kéo API và Kafka của '{config.data_type}'.")

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.__exit__(exc_type, exc_val, exc_tb) 
        if hasattr(self, 'trino_conn') and self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
        logger.info("🔒 Đã đóng kết nối Metadata Manager sau khi hoàn thành.")

            