import pandas as pd
from infrastructure.common import get_db_engine
import logging
import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
engine = get_db_engine()

class CheckpointManager:
    def __init__(self, filepath="state_crawled_stocks.json"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.filepath = os.path.join(current_dir, filepath)
        self.completed_stocks = set()
        self._load()
    
    def _load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.completed_stocks = set(data.get("done", []))
            logger.info(f"đã tải lên thành công {len(self.completed_stocks)} mã .")
        else:
            self.completed_stocks = set()
            

    def mark_done(self, symbol):
        """Ghi nhận 1 mã đã xong (Commit)"""
        self.completed_stocks.add(symbol)
        self._save()

    def _save(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
           json.dump({"done": list(self.completed_stocks)}, f)
    
    def filter_todo_list(self, tickers_list):
        todo = list(set(tickers_list) - self.completed_stocks)
        todo.sort()
        return todo
    
    
    
def get_ticker_list():
    try:
        df_tickers = pd.read_sql('SELECT DISTINCT("Ticker") FROM analysis_data.companies_list ORDER BY "Ticker" ASC', engine)
        ticker_list = set(df_tickers['Ticker'].str.strip())
        return ticker_list
    except Exception as e:
        logger.error(f"Error fetching ticker list: {e}")
        return []
    
    
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
    })
    return s

