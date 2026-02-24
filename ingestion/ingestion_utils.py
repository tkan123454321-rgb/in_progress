import time
import logging
import requests
import os
from dotenv import load_dotenv
import json
from utils.logger_config import setup_logger


load_dotenv()
logger = setup_logger(component="extract")


class CheckpointManager:
    
    def __init__(self, filepath="state_crawled_stocks.json"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.filepath = os.path.join(current_dir, filepath)
        self.completed_stocks = set()
        self._load()
    
    def _check_time(self):
        current_time = time.time() * 1000 
        file_time = os.path.getmtime(self.filepath) * 1000
        current_age = current_time - file_time
        return current_age
    
    
    def _load(self):
        
        if not os.path.exists(self.filepath):
            logger.info("Chưa có file checkpoint, sẽ tạo mới từ đầu.")
            self.completed_stocks = set()
            return
        
        if self._check_time() > 86400000:  # 24 hours in milliseconds
            logger.info("File checkpoint đã cũ hơn 24 giờ, sẽ tạo mới từ đầu.")
            self.completed_stocks = set()
            if os.path.exists(self.filepath):
                os.remove(self.filepath)
            return
        
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.completed_stocks = set(data.get("done", []))
            logger.info(f"đã tải lên thành công {len(self.completed_stocks)} mã .")
            
        except Exception as e:
            logger.error(f"Lỗi khi tải file checkpoint: {e}")
            self.completed_stocks = set()
            

    def mark_done(self, symbol):
        """Ghi nhận 1 mã đã xong (Commit)"""
        self.completed_stocks.add(symbol)
        self._save_atomic()

    def _save_atomic(self):
        temp_path = self.filepath + ".tmp"
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump({"done": list(self.completed_stocks)}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.filepath)
            logger.debug("Checkpoint saved successfully.")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def filter_todo_list(self, tickers_list):
        if len(self.completed_stocks) >= len(tickers_list):
            self.completed_stocks = set()
            if os.path.exists(self.filepath): 
                os.remove(self.filepath)
        todo = list(set(tickers_list) - self.completed_stocks)
        todo.sort()
        logger.info(f"Còn {len(todo)} mã chưa nạp báo cáo tài chính.")
        return todo
    
    
    

    



    
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

