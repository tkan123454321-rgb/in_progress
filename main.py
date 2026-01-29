from ingestion.utils import CheckpointManager, get_ticker_list
from utils.logger_config import setup_logger
from logging import getLogger
import logging
from ingestion.fetch_raw_financial_report import _fetch_financial_reports

logger = setup_logger(component="extract")
manager = CheckpointManager()
ticker_list = get_ticker_list()
todo_ticker_list = manager.filter_todo_list(ticker_list)


for ticker in todo_ticker_list:
    
   success = _fetch_financial_reports(ticker)
   if success:
       manager.mark_done(ticker)
       logger.info(f"Successfully fetched financial reports for {ticker}")