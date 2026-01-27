from utils.logger_config import setup_logger
from logging import getLogger
import logging

logger = setup_logger(__name__, level = logging.INFO)

for i in range(10):
    logger.info(f"tải xong kiki thứ {i}") 


