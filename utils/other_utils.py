import time
import logging
import requests
import os
from dotenv import load_dotenv
import json
from utils.logger_config import setup_logger
import requests

load_dotenv()


logger = setup_logger(component="utils")


# tạo 1 session duy trì kết nối  
def _get_session():
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
