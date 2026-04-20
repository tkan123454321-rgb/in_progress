import requests
import os
from common.core.logger_config import setup_logger
logger = setup_logger(component="utils")

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