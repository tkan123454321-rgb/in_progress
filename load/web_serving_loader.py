from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
from utils.logger_config import setup_logger
from utils.other_utils import map_trino_to_pg_type
from psycopg import sql
from typing import Tuple, Any

logger = setup_logger(component="load")

class WebServingLoader:
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        """
        Nhận 2 client đã được khởi tạo sẵn từ bên ngoài.
        Maintenance chỉ làm nhiệm vụ điều phối (Orchestrator).
        """
        self.pg_conn = pg_client.get_db_connection(db_name="ops_db")
        self.trino_conn = lake_client._get_trino_connection()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ủy quyền dọn dẹp cho thư viện gốc"""
        if self.pg_conn:
            self.pg_conn.__exit__(exc_type, exc_val, exc_tb)
            logger.info("🔒 Đã đóng kết nối tới PostgreSQL.")
        if self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
            logger.info("🔒 Đã đóng kết nối tới Trino.")
            
    def _extract_trino_payload(self)-> Tuple[str, str, list[tuple]]:
        """
        Hàm nội bộ: Chuyên đi khai thác Schema và Dữ liệu từ Trino.
        Trả về 3 món đồ chơi: final_sql_columns (để CREATE), column_names (để COPY), và rows (dữ liệu).
        """
        logger.info("🔍 Đang quét bản vẽ và dữ liệu từ Lakehouse (obt.obt_web)...")
        with self.trino_conn.cursor() as cur:
            # 1. Lấy Schema
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'obt_web'
                ORDER BY ordinal_position
            """)
            
            key_values = []
            col_names = []
            for col in cur.fetchall():
                col_name = col[0]
                data_type = map_trino_to_pg_type(col[1])
                
                key_values.append(f'"{col_name}" {data_type}')
                col_names.append(f'"{col_name}"') # Bọc nháy kép luôn để lát ném vào COPY
                
            final_sql_columns = ", ".join(key_values)
            copy_columns_str = ", ".join(col_names) # Ra dạng: "ticker", "qmj_score", ...

            # 2. Lấy Data
            cur.execute("SELECT * FROM obt.obt_web")
            rows = cur.fetchall()

        return (final_sql_columns, copy_columns_str, rows)
    
    def sync_obt_to_postgres(self):
        """hàm chính: Chuyên đi đồng bộ dữ liệu từ Trino về Postgres bằng cách tạo bảng tạm, COPY, rồi tráo bảng."""
        try:
            final_sql_columns, copy_columns_str, rows = self._extract_trino_payload()
            
            if not rows:
                logger.warning("⚠️ Bảng Lakehouse rỗng, dừng đồng bộ!")
                return

            logger.info("🔥 Bắt đầu nạp Postgres cho bảng web.web_obt...")
            with self.pg_conn.transaction():
                with self.pg_conn.cursor() as pg_cur:
                    
                    # 1. Xây bảng tạm web.web_obt_temp
                    pg_cur.execute("DROP TABLE IF EXISTS web.web_obt_temp")
                    pg_cur.execute(f"CREATE TABLE web.web_obt_temp ({final_sql_columns})")  # type: ignore
                    # 2. Xả lũ bằng COPY (Cú pháp SQL thuần túy, cực dễ đọc)
                    logger.info(f"🌪️ Đang xả lũ {len(rows)} dòng bằng COPY...")
                    copy_query = f"COPY web.web_obt_temp ({copy_columns_str}) FROM STDIN"
                    
                    with pg_cur.copy(copy_query) as copy_operation: # type: ignore
                        for row in rows:
                            copy_operation.write_row(row)

                    # 3. Đánh Index trên bảng tạm (Bác nhớ sửa tên cột theo ý muốn)
                    logger.info("⚡ Đang tạo Index...")
                    self._create_optimal_indexes(pg_cur)

                    # 4. Tráo Bảng
                    logger.info("🔀 Đang tráo bảng (Zero-Downtime Swap)...")
                    pg_cur.execute("DROP TABLE IF EXISTS web.web_obt_old")
                    
                    # Cất bảng cũ đi
                    pg_cur.execute("""
                        DO $$ 
                        BEGIN
                            IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'web' AND tablename = 'web_obt') THEN
                                ALTER TABLE web.web_obt RENAME TO web_obt_old;
                            END IF;
                        END $$;
                    """)
                    
                    # Đổi bảng tạm thành bảng chính
                    pg_cur.execute("ALTER TABLE web.web_obt_temp RENAME TO web_obt")
                    
            logger.info("🎉 THÀNH CÔNG! Dữ liệu đã lên sóng.")

        except Exception as e:
            logger.error(f"❌ Lỗi rồi: {e}")
            raise e
                       
    def _create_optimal_indexes(self, pg_cur):
        """
        Hardcode 100% cho bảng web.web_obt_temp.
        Không tự đặt tên Index để tránh lỗi trùng lặp khi Swap bảng.
        """
        logger.info("⚡ Đang xây dựng hệ thống Index siêu tốc cho bộ lọc Web...")
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("ticker")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("year", "quarter")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("qmj_rank")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("z_value_recent")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("z_momentum_recent")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("year", "quarter", "qmj_rank")')
        logger.info("✅ Đã đánh Index xong! Bảng tạm sẵn sàng lên sóng.")

                
                 
                    
