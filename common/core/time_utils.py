from datetime import date, datetime, timedelta
from typing import Tuple, Literal
import math

from zoneinfo import ZoneInfo


def get_target_anchor(data_type : Literal["quarter", "year"]) -> Tuple[int, int]:
    """
    Calculates the target year and quarter to act as a watermark anchor for data ingestion.

    Financial data (like earnings reports) is subject to reporting lags. Therefore, 
    this function intentionally steps back to the *previous* completed quarter or year 
    relative to the current date. This ensures the pipeline targets data that has 
    likely been published, rather than attempting to fetch data for an ongoing period.

    Args:
        data_type (Literal["quarter", "year"]): The frequency of the financial data.

    Returns:
        Tuple[int, int]: A tuple containing (target_year, target_quarter). 
                         If the data_type is "year", the quarter defaults to 0.
                         
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
    Calculates the baseline starting year for fetching historical data.

    This acts as a safe fallback when a stock ticker is newly discovered and has 
    no prior watermark in the database. It prevents the pipeline from attempting 
    to fetch decades of potentially irrelevant data by capping the history.

    Args:
        lookback_years (int, optional): The number of historical years to fetch. Defaults to 8.

    Returns:
        int: The calculated fallback year (Current UTC Year minus lookback_years).
    """
    return datetime.now(ZoneInfo("UTC")).year - lookback_years