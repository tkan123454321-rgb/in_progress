import re
from schema.producer_schema import BaseMetadata
from dotenv import load_dotenv
from pathlib import Path
import requests
import os
from dotenv import load_dotenv
import yaml # type: ignore
from utils.logger_config import setup_logger
from pydantic import BaseModel, ValidationError
import requests
from utils.lakehouse_client import LakeHouseClient
from utils.postgres_client import PostgresClient
from typing import Any, Callable, ClassVar, Dict
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

