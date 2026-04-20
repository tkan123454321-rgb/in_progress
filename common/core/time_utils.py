from datetime import date, datetime, timedelta
from typing import Tuple, Literal
import math
from common.core.logger_config import setup_logger
from zoneinfo import ZoneInfo

logger = setup_logger(component="utils")

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