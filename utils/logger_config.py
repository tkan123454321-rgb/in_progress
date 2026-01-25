from logging import *
from logging.handlers import RotatingFileHandler
import os
import sys


def setup_logger():
    # tạo thư mục chứa log nếu chưa có
    log_file_path = "logs/pipeline.log"
    log_dir = os.path.dirname(log_file_path) 
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
        
    # tạo logger instance / cài config logger    
    logger = getLogger()
    logger.setLevel(INFO)
    logger.handlers.clear()  # Xoá hết handler cũ để tránh trùng lặp log
    formatter = Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    # cấu hình Console handler streaming 
    console_handler = StreamHandler(sys.stdout)
    console_handler.setLevel(INFO)
    console_handler.setFormatter(formatter)
    
    # cấu hình file handler
    file_handler = RotatingFileHandler(
        filename='logs/trading.log', 
        mode='a', 
        maxBytes=10*1024*1024,    # 10 MB
        backupCount=3, 
        encoding='utf-8',        # tiếng việt
        delay=False)
    file_handler.setLevel(INFO)
    file_handler.setFormatter(formatter)
    
    # thêm file_handler vào logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
