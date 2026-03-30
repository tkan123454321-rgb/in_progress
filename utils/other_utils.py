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
