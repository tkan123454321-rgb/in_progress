import math
import requests
import os
from dotenv import load_dotenv
from utils.logger_config import setup_logger
import requests
from typing import Any, Callable, ClassVar, Dict
from typing import Iterable, List, Optional, Tuple, TypeVar, Union, Literal
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
logger = setup_logger(component="utils")

    
# tạo 1 session duy trì kết nối  
def _get_session() -> requests.Session:
    s = requests.Session()
    # lấy các biến từ .env
    auth_token = os.getenv('AUTH_TOKEN')
    user_agent = os.getenv('USER_AGENT')
    source = os.getenv('MY_SOURCE')
    if not auth_token or not user_agent or not source:
        logger.critical("THIẾU CONFIG! Kiểm tra lại file .env")
    s.headers.update({
    "User-Agent": user_agent,
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://{source}.vn/",
    "Authorization": auth_token
    }) # type: ignore
    return s

def get_target_anchor(data_type : Literal["quarter", "year"]) -> Tuple[int, int]:
        """
        Tính toán Năm và Quý mục tiêu dựa trên ngày hiện tại.
        Luôn lùi 1 quý so với quý hiện tại để làm mốc kéo dữ liệu.
        """
        if data_type == "quarter":
            today = date.today()
            current_quarter = math.ceil(today.month / 3)
            
            if current_quarter == 1:
                return today.year - 1, 4
            else:
                return today.year, current_quarter - 1
        elif data_type == "year":
            today = date.today()
            return today.year - 1, 0
        
def get_fallback_year(lookback_years: int = 8) -> int:
    """
    Tính toán năm làm mốc (fallback) dựa trên năm hiện tại.
    Mặc định lùi lại 8 năm.
    """
    return datetime.now(ZoneInfo("UTC")).year - lookback_years

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
