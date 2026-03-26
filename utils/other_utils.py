import math
import requests
import os
from dotenv import load_dotenv
from utils.logger_config import setup_logger
import requests
from typing import Any, Callable, ClassVar, Dict
from typing import Iterable, List, Optional, Tuple, TypeVar, Union
from datetime import date
logger = setup_logger(component="utils")

    
# tạo 1 session duy trì kết nối  
def _get_session() -> requests.Session:
    s = requests.Session()
    # lấy các biến từ .env
    auth_token = os.getenv('AUTH_TOKEN')
    user_agent = os.getenv('USER_AGENT')
    if not auth_token or not user_agent:
        logger.critical("THIẾU CONFIG! Kiểm tra lại file .env")
    s.headers.update({
    "User-Agent": user_agent,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://fireant.vn/",
    "Authorization": auth_token
    }) # type: ignore
    return s

def get_target_anchor() -> Tuple[int, int]:
        """
        Tính toán Năm và Quý mục tiêu dựa trên ngày hiện tại.
        Luôn lùi 1 quý so với quý hiện tại để làm mốc kéo dữ liệu.
        """
        today = date.today()
        current_quarter = math.ceil(today.month / 3)
        
        if current_quarter == 1:
            return today.year - 1, 4
        else:
            return today.year, current_quarter - 1
