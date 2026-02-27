import psycopg
from utils.logger_config import setup_logger
import os
from datetime import datetime
import polars as pl
from polars.exceptions import PolarsError, ColumnNotFoundError, SQLInterfaceError, SQLSyntaxError

logger = setup_logger(component="utils")


USER = os.getenv("POSTGRES_USER")
PASS = os.getenv("POSTGRES_PASSWORD")
CONN_STR = f"postgresql://{USER}:{PASS}@postgres:5432/ops_db"

def get_db_connection():
    """
    Hàm tự động tạo kết nối đến Postgres.
    Trả về đối tượng connection để dùng với từ khóa 'with'.
    """
    
    try:
        conn = psycopg.connect(conninfo=CONN_STR, autocommit=True)
        return conn
    except psycopg.Error as e:
        logger.critical(f"🔥 Không thể kết nối DB| mã lỗi: {e.sqlstate} {e}")
        raise



def log_metadata_to_db(conn, table_name, batch_id, topic, ticker, data_type):
    sql = f"""
        INSERT INTO ingestion.{table_name} 
        (batch_id, topic_name, data_type, ticker, created_time)
        VALUES (%(batch_id)s, %(topic)s, %(data_type)s, %(ticker)s, %(created_time)s)
    """
    
    data = {
        "batch_id": batch_id,
        "topic": topic,
        "data_type": data_type,
        "ticker": ticker,
        "created_time": datetime.now()
    }
    
    try:
        conn.execute(sql, data)
    except psycopg.Error as e:
        # Bắt ĐÍCH DANH lỗi của psycopg (Database Error)
        # Báo warning để vòng lặp đi tiếp, không làm chết luồng chạy của Kafka
        logger.error(f"⚠️ Lỗi Database khi ghi log cho mã {ticker}: {e.sqlstate}, {e}")
        raise



        
def cleanup_state_table(conn, table_name):
    """BƯỚC 1: Chạy 1 lần ở đầu pipeline - Xóa sạch bảng nếu dữ liệu cũ hơn 24h"""
    cleanup_sql = f"""
        DO $$
        BEGIN
            IF (SELECT MAX(created_time) FROM ingestion.{table_name}) < NOW() - INTERVAL '24 HOURS' THEN
                TRUNCATE TABLE ingestion.{table_name};
            END IF;
        END $$;
    """
    try:
        conn.execute(cleanup_sql)
        logger.info(f"🧹 Đã kiểm tra và dọn dẹp bảng {table_name} (nếu dữ liệu quá 24h).")
    except psycopg.Error as e:
        logger.error(f"⚠️ Lỗi dọn dẹp DB bảng {table_name}: {e}")





def get_missing_tickers(conn_str, table_name, tickers_set):
    """BƯỚC 2: Quét DB bằng Polars tốc độ cao để tìm các mã chưa được nạp"""
    query = f"SELECT ticker FROM ingestion.{table_name}"
    try:
        df = pl.read_database_uri(query=query, uri=conn_str, engine="connectorx")
        
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