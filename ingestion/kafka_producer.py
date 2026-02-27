import logging
from confluent_kafka import Producer
from confluent_kafka.error import ProduceError
from logging import *
from utils.logger_config import setup_logger
from confluent_kafka.admin import AdminClient, NewTopic  # pyright: ignore[reportPrivateImportUsage]
import os
import time
import pandas as pd
from utils.logger_config import setup_logger

# cài đặt logger
logger = setup_logger(component="extract")   
# Cấu hình Kafka Producer

# hàm kiểm tra và tạo topic 
    
class StockTickerProducer:
    def __init__(self, bootstrap_servers, 
                 client_id, 
                 topic_name, 
                 update_conf: dict[str, str | int | float | bool] | None = None):
        
        self.client_id = client_id
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name

        # 1. Cấu hình Producer
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': client_id,
            'enable.idempotence': True, # Đảm bảo tin không bị duplicate
            'compression.type': 'lz4',      # Nén để giảm dung lượng mạng
            'linger.ms': 50             # Chờ 50ms để gom batch
        }
        
        if update_conf:
            self.conf.update(update_conf)
            
        logger.debug(f"Cấu hình Producer: {self.conf}")
        
        # 2. Khởi tạo Resource
        self.producer = Producer(self.conf)   



    # Hàm báo cáo kết quả gửi tin nhắn
    def _on_delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"gửi thất bại {err}", exc_info=True)
        else:
            logger.debug(f"nạp Thành công {msg.value().decode('utf-8')} | offset: {msg.offset()} |topic: {msg.topic()} | partition: {msg.partition()} | client_id: {self.conf['client.id']}")




    def batch_message_tickers(self, messages, topic_name):
        logger.info(f"Đang nạp {len(messages)} mã", extra = {'client_id': self.conf['client.id']} )
        retry_delay = 0.2
        attempt = 0
        while True:
            try:
                self.producer.produce_batch(
                    topic=topic_name,
                    messages=messages,
                    on_delivery=self._on_delivery_report,
                )
                msg_number = self.producer.flush(5)
                if msg_number == 0:
                    logger.info(f"Hoàn tất nạp: còn {msg_number} mã vẫn ở hàng đợi")
                else: 
                    logger.warning(f"nạp thất bại: còn {msg_number} mã vẫn ở hàng đợi", exc_info=True)
                return True

            except TypeError as e:
                logger.error(f" Lỗi: {e}, sai định dạng messages", exc_info=True)
                return False
            except BufferError as e:
                logger.warning(f" Lỗi: {e}, Buffer đầy, đang chờ {retry_delay}s để worker dọn dẹp.")
                self.producer.poll(retry_delay)
            except ProduceError as e:
                kafka_error = e.args[0]
                if kafka_error.retriable():
                    attempt += 1
                    if attempt > 5:
                        logger.error(f" Lỗi: {kafka_error}, đã vượt quá số lần thử lại", exc_info=True)
                        return False
                    logger.warning(f" Lỗi có thể retry, đang chờ {retry_delay}s trước khi thử lại.")
                    time.sleep(retry_delay)
                    self.producer.poll(0)
                else:
                    logger.error(f" Lỗi không thể retry: {kafka_error}", exc_info=True)
                    return False
            except Exception as e:
                logger.error(f" Lỗi không xác định: {e}", exc_info=True)
                return False

            finally:
                self.producer.flush()

    def single_message_data(self, message, key, topic_name):
        retry_delay = 0.2
        attempt = 0 
        while True:
            try:
                self.producer.produce(
                    topic=topic_name,
                    value=message,
                    on_delivery=self._on_delivery_report,
                    key=key
                )
                self.producer.poll(0)
                logger.debug(f"Đã gửi tin nhắn với key: {key} đến topic: {topic_name}", extra = {'client_id': self.conf['client.id']} )
                return True
            except BufferError as e:
                logger.warning(f" Lỗi: {e}, Buffer đầy, đang chờ {retry_delay}s để worker dọn dẹp.")
                self.producer.poll(retry_delay)
            except ProduceError as e:
                kafka_error = e.args[0]
                if kafka_error.retriable():
                    attempt += 1
                    if attempt > 5:
                        logger.error(f" Lỗi: {kafka_error}, đã vượt quá số lần thử lại", exc_info=True)
                        return False
                    logger.warning(f" Lỗi có thể retry, đang chờ {retry_delay}s trước khi thử lại.")
                    time.sleep(retry_delay)
                    self.producer.poll(0)
                else:
                    logger.error(f" Lỗi không thể retry: {kafka_error}", exc_info=True)
                    return False
                
    def close(self):
        # Hàm này quan trọng để flush nốt dữ liệu thừa trước khi tắt
        logger.info("flushing producer before closing...")
        self.producer.flush(10)
            
            