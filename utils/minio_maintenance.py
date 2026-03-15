from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
from utils.logger_config import setup_logger
logger = setup_logger(component="minio_maintenance")




def minio_maintenance(db_name: str ="platform_db") -> None:
    lakehouse = LakeHouseClient()
    with PostgresClient.get_db_connection(db_name=db_name) as conn:
        pg_client = PostgresClient(conn=conn)
        
        try:
            # Vẫn bọc trong transaction để bảo vệ toàn vẹn dữ liệu
            with pg_client.conn.transaction():
                
                # 1. Tạo bảng tạm
                pg_client.prepare_temp_minio_table()
                for batch in lakehouse.yield_schema_locations():
                    pg_client.insert_minio_location_batch(batch)
                pg_client.swap_minio_location_tables()
            logger.info("Hoàn thành nạp minio object locations vào PostgreSQL")
            total_deleted = 0
            for orphan_loc in pg_client.yield_orphan_location_batches():
                lakehouse.clean_orphan_location(orphan_loc)
                total_deleted += 1
            if total_deleted == 0:
                logger.info("Hệ thống sạch sẽ, không có rác nào cần dọn!")
            else:
                logger.info(f"HOÀN TẤT GC! Tổng cộng đã tiêu diệt: {total_deleted} thư mục rác.")         
        except Exception as e:
            logger.error(f" CÓ LỖI, ĐÃ ROLLBACK: {e}")
            

