import json
from logging import *
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
import logging_loki
from queue import Queue


class JsonFormatter(Formatter):
    def format(self, record):
        vn_timezone = timezone(timedelta(hours=7))
        dt_object = datetime.fromtimestamp(record.created, vn_timezone)
        vn_time_str = dt_object.strftime('%Y-%d-%m %H:%M:%S')
        
        log_record = {
            "level": record.levelname,
            "name": record.name,
            "path_name": record.pathname,
            "message": record.getMessage(),
            "time": vn_time_str,
            "error": self.formatException(record.exc_info) if record.exc_info else None
        }
        return json.dumps(log_record,ensure_ascii=False)

def setup_logger(name: str, level: int = DEBUG):
    logger = getLogger(name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    handler = logging_loki.LokiQueueHandler(
        Queue(-1), 
        url="http://loki:3100/loki/api/v1/push", 
        tags={"app": "dev-container"}, 
        auth=None, 
        version="1",
    )
    console_handler = StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    console_handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.addHandler(console_handler)

    return logger