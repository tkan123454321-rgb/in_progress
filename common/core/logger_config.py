import json
from logging import getLogger, LoggerAdapter, Formatter, StreamHandler, Logger
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict
from unittest import result
import logging_loki
from queue import Queue


global_level = logging.INFO

class JsonFormatter(Formatter):
    
    def formatException(self, exc_info):
        """
        Format an exception so that it prints on a single line.
        """
        result = super().formatException(exc_info)
        result = repr(result)
        return result.replace('\n', '') + '|'


    def format(self, record : logging.LogRecord) -> str:
        vn_timezone = timezone(timedelta(hours=7))
        dt_object = datetime.fromtimestamp(record.created, vn_timezone)
        vn_time_str = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    
    
        log_record = {
            "level": record.levelname,
            "path_name": record.pathname,
            "func_name": record.funcName,   
            "line_no": record.lineno,       
            "message": record.getMessage(),
            "time": vn_time_str,
            "error": self.formatException(record.exc_info) if record.exc_info else None
        }
        return json.dumps(log_record, ensure_ascii=False)

def setup_logger(component: str, env: str = "dev", **kwargs) -> LoggerAdapter:
    """
    Initializes and configures a centralized logger for the data pipeline.

    This function sets up a logger with two main handlers:
    1. LokiQueueHandler: Asynchronously pushes logs to a Loki with specific tags.
    2. StreamHandler: Outputs JSON-formatted logs to standard Console output.
    Args:
        component (str): The name of the pipeline component or module (e.g., 'ingestion', 'transformation').
        env (str, optional): The execution environment (e.g., 'dev', 'prod', 'staging'). Defaults to "dev".
        **kwargs: Additional contextual information. 
                  - Expected key: `run_id` (str) to track specific Airflow DAG runs or execution instances.

    Returns:
        logging.LoggerAdapter: A configured logger instance wrapped in an adapter, 
                               containing contextual extras (component, env, run_id).
    """
    logger = getLogger(f"pipeline.{component}")
    logger.setLevel(global_level)
    
    if not logger.hasHandlers():
    
        handler = logging_loki.LokiQueueHandler(
            Queue(-1), 
            url="http://loki:3100/loki/api/v1/push", 
            tags={"app": "main_pipeline", "component": component, "env": env, "run_id": kwargs.get("run_id")}, 
            auth=None, 
            version="1",
        )
        console_handler = StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        console_handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.addHandler(console_handler)

    return logging.LoggerAdapter(logger, {"component": component, "env": env, "run_id": kwargs.get("run_id")})

