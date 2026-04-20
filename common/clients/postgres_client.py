import psycopg
from psycopg.rows import dict_row, DictRow
from common.core.logger_config import setup_logger
import os
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
    
def map_trino_to_pg_type(trino_type: str) -> str:
    """
    Máy dịch kiểu dữ liệu từ Trino sang Postgres.
    Input: col[1] (Ví dụ: 'double', 'varchar', 'decimal(20,4)')
    Output: Kiểu Postgres chuẩn (Ví dụ: 'DOUBLE PRECISION', 'TEXT', 'DECIMAL(20,4)')
    """
    # Xóa khoảng trắng thừa và đưa về chữ thường để dễ so sánh
    t = trino_type.lower().strip()

    # 1. Nhóm Chuỗi (String)
    # Bất kể là varchar, varchar(255) hay char, cứ ép hết về TEXT cho an toàn,
    # Postgres xử lý TEXT cực nhanh và không bao giờ lo bị cắt xén dữ liệu.
    if t.startswith('varchar') or t.startswith('char'):
        return 'TEXT'

    # 2. Nhóm Số thực (Floating Point) - Kẻ thù số 1
    if t == 'double':
        return 'DOUBLE PRECISION'
    if t == 'real':
        return 'REAL'

    # 3. Nhóm Số chính xác (Decimal/Numeric)
    # Nó có dạng decimal(20,4). Mình chỉ cần in hoa lên là Postgres hiểu y xì đúc.
    if t.startswith('decimal') or t.startswith('numeric'):
        return t.upper()

    # 4. Nhóm Ngày Giờ (Date/Time)
    if t.startswith('timestamp'):
        if 'with time zone' in t:
            return 'TIMESTAMP WITH TIME ZONE'
        return 'TIMESTAMP'
    
    if t == 'date':
        return 'DATE'

    # 5. Nhóm Số nguyên (Integer)
    if t == 'bigint':
        return 'BIGINT'
    if t in ('integer', 'int'):
        return 'INTEGER'
    if t in ('smallint', 'tinyint'):
        return 'SMALLINT'

    # 6. Boolean (Đúng/Sai)
    if t == 'boolean':
        return 'BOOLEAN'

    return 'TEXT'


     
    
   
        
        
   