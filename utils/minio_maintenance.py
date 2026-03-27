from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
from utils.logger_config import setup_logger
from datetime import datetime, timedelta, timezone
from psycopg import sql
from typing import Sequence, Generator
from trino.exceptions import Error as TrinoError
logger = setup_logger(component="minio_maintenance")


class LakehouseMaintenance:
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        """
        Nhận 2 client đã được khởi tạo sẵn từ bên ngoài.
        Maintenance chỉ làm nhiệm vụ điều phối (Orchestrator).
        """
        self.pg_conn = pg_client.get_db_connection(db_name="platform_db")
        self.trino_conn = lake_client._get_trino_connection()
        self.s3_client = lake_client.s3_client
        self.catalog = lake_client.catalog
        self.REQUIRED_SCHEMAS = lake_client.REQUIRED_SCHEMAS
        self.BUCKET_NAME = lake_client.BUCKET_NAME
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ủy quyền dọn dẹp cho thư viện gốc"""
        if self.pg_conn:
            self.pg_conn.__exit__(exc_type, exc_val, exc_tb)
        if self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
        logger.info("🔒 Đã đóng toàn bộ kết nối bảo trì Lakehouse.")
    
    def _prepare_temp_minio_table(self) -> None:
        """BƯỚC 1: Dọn dẹp và tạo bảng Tmp. Không cần cursor nữa."""
        setup_sql = """
            DROP TABLE IF EXISTS nessie_gc.temp_minio_object_locations;
            CREATE TABLE nessie_gc.temp_minio_object_locations (
                location VARCHAR PRIMARY KEY,
                scanned_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """
        # Quất thẳng execute từ connection
        self.pg_conn.execute(setup_sql)
        logger.info("Đã tạo bảng tạm: temp_minio_object_locations.")

    def _insert_minio_location_batch(self, batch: Sequence[tuple]) -> None:
        """BƯỚC 2: Nạp 1 batch dữ liệu vào bảng Tmp."""
        insert_sql = "INSERT INTO nessie_gc.temp_minio_object_locations (location) VALUES (%s)"
        # executemany cũng gọi thẳng từ connection luôn
        with self.pg_conn.cursor() as cur:
            cur.executemany(insert_sql, batch)

    def _swap_minio_location_tables(self) -> None:
        """BƯỚC 3: Tráo bảng Tmp thành bảng Chính."""
        swap_sql = """
            DROP TABLE IF EXISTS nessie_gc.minio_object_locations CASCADE;
            ALTER TABLE nessie_gc.temp_minio_object_locations RENAME TO minio_object_locations;
        """
        self.pg_conn.execute(swap_sql)
        logger.info("🔄 Đã Swap thành công sang bảng chính: minio_object_locations.")
    
    def _yield_orphan_location_batches(self) -> Generator[str, None, None]:
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
        
        with self.pg_conn.transaction():
            self.pg_conn.execute(set_ram_sql)
            with self.pg_conn.cursor(name="orphan_stream_cursor") as cur:
                # Ép RAM cho query
                # Thực thi EXCEPT
                cur.execute(query)
                
                # Bơm nước trả về từng mẻ
                while True:
                    batch = cur.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        yield row['location'] # type: ignore
        
    def _prepare_maintenance_queue(self):
        
        query_create_table = sql.SQL("""
                        CREATE TABLE IF NOT EXISTS iceberg_maintenance.iceberg_optimize (
                            schema_name VARCHAR(100),
                            table_name VARCHAR(255),
                            status VARCHAR(20) DEFAULT 'PENDING',
                            batch_created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            last_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (schema_name, table_name)
            )
            """)
        

        # Bước 1: kiểm tra và tạo bảng nếu chưa có
        self.pg_conn.execute(query_create_table)
        cursor_check = self.pg_conn.execute("SELECT COUNT(*) as total_count, MIN(batch_created_at) as oldest_date FROM iceberg_maintenance.iceberg_optimize;")
        row = cursor_check.fetchone()
        print(row)
        if row['total_count'] == 0: # type: ignore
            logger.info("Bảng chưa có dữ liệu, chuẩn bị nạp metadata mới.")
            return True 
            
        if row['oldest_date'] is not None: # type: ignore
            if row['oldest_date'] < datetime.now(timezone.utc) - timedelta(days=7):  # type: ignore
                logger.info("🔄 Đã qua 7 ngày. Đang dọn dẹp hàng đợi cũ (TRUNCATE)...")
                self.pg_conn.execute("TRUNCATE TABLE iceberg_maintenance.iceberg_optimize;")
                return True
                
        return False
    
    def _yield_schema_locations(self) -> Generator[Sequence[tuple], None, None]:
        """
        Quét MinIO theo từng layer, trả về generator chứa các batch thư mục con.
        Định dạng trả về: (tên_schema, [(location1,), (location2,), ...])
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for schema_name in self.REQUIRED_SCHEMAS:
            prefix_path = f"{schema_name}/"
            page_paginator = paginator.paginate(Bucket=self.BUCKET_NAME, Delimiter='/', Prefix=prefix_path)
            for page in page_paginator:
                if 'CommonPrefixes' in page:
                    batch=[]
                    for obj in page['CommonPrefixes']:
                        folder_path = obj['Prefix']
                        batch.append((folder_path,))
                    yield batch

    def _clean_orphan_location(self, location: str) -> None:
        """
        Xóa một thư mục mồ côi trên MinIO.
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.BUCKET_NAME, Prefix=location)
        for page in pages:
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                for obj in page['Contents']:
                    response = self.s3_client.delete_objects(Bucket=self.BUCKET_NAME, 
                                                            Delete={
                                                                'Objects': objects_to_delete,
                                                                'Quiet': True
                                                            })
                    if 'Errors' in response:
                        logger.error(f"❌ Lỗi khi xóa file trong {location}: {response['Errors']}")

    def minio_maintenance(self) -> None:
        try:
            # Vẫn bọc trong transaction để bảo vệ toàn vẹn dữ liệu
            with self.pg_conn.transaction():
                
                # 1. Tạo bảng tạm
                self._prepare_temp_minio_table()
                for batch in self._yield_schema_locations():
                    self._insert_minio_location_batch(batch)
                self._swap_minio_location_tables()
            logger.info("Hoàn thành nạp minio object locations vào PostgreSQL")
            total_deleted = 0
            for orphan_loc in self._yield_orphan_location_batches():
                self._clean_orphan_location(orphan_loc)
                total_deleted += 1
            if total_deleted == 0:
                logger.info("Hệ thống sạch sẽ, không có rác nào cần dọn!")
            else:
                logger.info(f"HOÀN TẤT GC! Tổng cộng đã tiêu diệt: {total_deleted} thư mục rác.")         
        except Exception as e:
            logger.exception(f" CÓ LỖI, ĐÃ ROLLBACK")
    
    def _refresh_queue_from_trino(self) -> None:
        with self.trino_conn.cursor() as cur:
            cur.execute("SELECT table_schema, table_name FROM system.iceberg_tables")
            rows = cur.fetchall()
        
        logger.info(f"🚚 [Postgres] Đang nạp {len(rows)} bảng vào Hàng đợi...")
        with self.pg_conn.cursor() as pg_cur:
            pg_cur.executemany("""
                INSERT INTO iceberg_maintenance.iceberg_optimize (schema_name, table_name) 
                VALUES (%s, %s)
                ON CONFLICT (schema_name, table_name) DO NOTHING
            """, rows)
        logger.info("✅ Hoàn thành nạp hệ thống bảng iceberg mới từ metadata vào bảng maintenance từ Trino.")
    
    def _run_optimize(self) -> None:
        """Đọc hàng đợi PENDING và điều phối chạy Optimize"""
        logger.info("BẮT ĐẦU XỬ LÝ HÀNG ĐỢI OPTIMIZE...")
        
        cursor = self.pg_conn.execute("""
            SELECT schema_name, table_name 
            FROM iceberg_maintenance.iceberg_optimize 
            WHERE status = 'PENDING'
        """)
        tasks = cursor.fetchall() 
        if not tasks:
            logger.info("🎉 Không có bảng PENDING nào. Mọi thứ đã SUCCESS!")
            return
        print(tasks)

        with self.trino_conn.cursor() as trino_cur:
            logger.info(f"🚀 Bắt đầu optimize {len(tasks)} bảng...")
            for task in tasks:
                try:
                    schema = task['schema_name'] # type: ignore
                    table = task['table_name'] # type: ignore
                    table_full_name = f"{schema}.{table}" 
                    trino_cur.execute(f"ALTER TABLE {table_full_name} EXECUTE optimize(file_size_threshold => '128MB')")
                    trino_cur.fetchall() # Nhớ phải có fetchall() để Trino xả hết kết quả nhé
                    
                    trino_cur.execute(f"ALTER TABLE {table_full_name} EXECUTE optimize_manifests")
                    trino_cur.fetchall()

                    update_query = """
                        UPDATE iceberg_maintenance.iceberg_optimize 
                        SET status = 'SUCCESS', last_updated_at = CURRENT_TIMESTAMP 
                        WHERE schema_name = %s AND table_name = %s
                    """
                    self.pg_conn.execute(update_query, (schema, table))
                    
                    logger.info(f"   ✅ BẢO TRÌ HOÀN TẤT: {table_full_name}")
                    
                except TrinoError as e:
                    logger.error(f"   ❌ [TRINO LỖI] Bỏ qua bảng {table_full_name}. Chi tiết: {e}")
    
    def run_full_iceberg_maintenance(self) -> None:
        """
        Hàm DUY NHẤT cần gọi từ bên ngoài. 
        Tự động kiểm tra hàng đợi, cập nhật danh sách và chạy nén file.
        """
        logger.info("🔥 BẮT ĐẦU QUY TRÌNH BẢO TRÌ ICEBERG LAKEHOUSE 🔥")
        try:
            # Bước 1: Tổ trưởng Kho kiểm tra xem có cần lấy danh sách mới không
            needs_refresh = self._prepare_maintenance_queue()
            
            # Bước 2: Nếu cần, gọi Tổ trưởng Vận chuyển đi lấy data từ Trino
            if needs_refresh:
                self._refresh_queue_from_trino()
                
            # Bước 3: Gọi Tổ trưởng Thi công ra mặt để nén file
            self._run_optimize()
            
            logger.info("QUY TRÌNH BẢO TRÌ iceberg tables thành công")
            
        except Exception as e:
            # Lưới bọc an toàn cuối cùng cho toàn bộ hệ thống
            logger.critical(f"💥 [CRITICAL ALERT] Sập toàn bộ quy trình bảo trì. Lý do: {e}")
                
                
        

                

