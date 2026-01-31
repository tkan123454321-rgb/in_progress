from ingestion.ingestion_utils import _get_session
from utils.db_connection import get_db_engine
import requests
import logging
import time
from requests.exceptions import RequestException, HTTPError, Timeout
from ingestion.kafka_adapter import StockTickerProducer
import json
from utils.logger_config import setup_logger

logger = setup_logger(component="extract")
engine = get_db_engine()
s = _get_session()

# khai báo producer instance
producer = StockTickerProducer(update_conf = {'compression.type': 'lz4', 'linger.ms': 50})



def _fetch_financial_reports(ticker: str, topic_name: str):
    start = 2018
    end = 2025
    years = range(end, start-1, -1)
    quarters = [4,3,2,1]
    
    try:
        for y in years: # tạo danh sách url tự động từ năm end đến start
            for q in quarters:
                url = f'https://restv2.fireant.vn/symbols/{ticker}/full-financial-reports?type=1&year={y}&quarter={q}&limit=1' # URL API
                try:
                    response = s.get(url, timeout=10) # Gửi yêu cầu GET
                    response.raise_for_status()
                    data = response.json()
                    if data:
                        msg = json.dumps(data, ensure_ascii=False).encode('utf-8')
                        key = f"{ticker}-{y}-{q}"
                        producer.single_message_data(message=msg, key=key, topic_name=topic_name)
                        logger.info(f"Nạp báo cáo tài chính thành công cho {ticker} năm {y} quý {q}")
                    time.sleep(0.1) # Thời gian chờ giữa các yêu cầu để tránh bị chặn
                except HTTPError as e:
                    logger.error(f"HTTP error for {ticker} in {y}-{q}: {e.response.status_code} - {e.response.text}")
                    continue
                except (Timeout, RequestException) as e:
                    logger.error(f"Error fetching data for {ticker} in {y}-{q}: {e}")
                    continue
        if producer.producer.flush(3) == 0:
            return True
        else: 
            logger.critical(f"nạp báo cáo tài chính thất bại cho {ticker}: còn dữ liệu trong hàng đợi", exc_info=True)
            return False
        
    except KeyboardInterrupt:
        logger.warning(f" đang xử lý{ticker}, nhận lệnh dừng từ bàn phím (KeyboardInterrupt).")
        raise 
    finally:
        leftover = producer.producer.flush(3) 
        
        if leftover > 0:
            logger.warning(f"⚠️ Cảnh báo: Còn {leftover} tin nhắn chưa kịp đẩy xuống Kafka sau 3s.")
        else:
            logger.debug(f"✅ Đã flush sạch hàng đợi cho {ticker}.")
    