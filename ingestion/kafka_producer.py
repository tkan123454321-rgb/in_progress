import logging
import socket
from typing import Any
from utils.kafka_client import ClassVar, KafkaClient
from confluent_kafka import Producer
from confluent_kafka.error import ProduceError
from utils.logger_config import setup_logger
import os
import time
import pandas as pd
from utils.logger_config import setup_logger
from contextlib import contextmanager

# cài đặt logger
logger = setup_logger(component="extract")   
# Cấu hình Kafka Producer

# hàm kiểm tra và tạo topic 
    
class StockTickerProducer(KafkaClient):
    _PRODUCER_CONFIG: ClassVar[dict[str, Any]] = {
            'enable.idempotence': True, # Đảm bảo tin không bị duplicate
            'compression.type': 'lz4',      # Nén để giảm dung lượng mạng
            'linger.ms': 50             # Chờ 50ms để gom batch 
        }
    def __init__(self,
                 topic_name: str, 
                 update_conf: dict[str, str | int | float | bool] | None = None):
        super().__init__(topic_name)

        pid = os.getpid()
        hostname = socket.gethostname()
        self.topic_name = topic_name
        self.client_id = f"stock-producer-{hostname}-{pid}"
        # 1. Cấu hình Producer
        self.conf = {
            'bootstrap.servers': self.DEFAULT_BOOTSTRAP,  # Sử dụng cấu hình mặc định từ KafkaClient
            'client.id': self.client_id,
            ** self._PRODUCER_CONFIG,  # Thêm cấu hình mặc định của Producer
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

    def single_message_data(self, message, key):
        retry_delay = 0.2
        attempt = 0 
        while True:
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    value=message,
                    on_delivery=self._on_delivery_report,
                    key=key
                )
                self.producer.poll(0)
                logger.debug(f"Đã gửi tin nhắn với key: {key} đến topic: {self.topic_name}", extra = {'client_id': self.conf['client.id']} )
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
            
    @classmethod
    @contextmanager
    def managed(cls, topic_name: str, update_conf: dict | None = None):
        """
        Factory method bọc Class lại thành một Context Manager.
        Sử dụng: with StockTickerProducer.managed("my_topic") as producer:
        """
        logger.info("🔌 [Kafka] Khởi tạo và mở kết nối Producer...")
        # 1. SETUP: Tạo đối tượng
        producer_instance = cls(topic_name=topic_name, update_conf=update_conf)
        
        try:
            # 2. YIELD: Nhả cái object ra cho khối 'with' xài
            yield producer_instance
            
        finally:
            if producer_instance.close() is None:
                logger.info("🔒 Đã đóng producer sau khi nạp metadata cơ bản thành công.")
            else:
                logger.critical("⚠️ Chưa flush hết dữ liệu khi đóng producer sau nạp metadata cơ bản.")