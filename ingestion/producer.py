from confluent_kafka import Producer
from confluent_kafka.error import ProduceError
from logging import *
from infrastructure.common import get_db_engine
from utils.logger_config import setup_logger
from confluent_kafka.admin import AdminClient, NewTopic
import os
import time
import pandas as pd
from ingestion.utils import get_ticker_list

# cài đặt logger
logger = getLogger(__name__)    

# Database connection setup
engine = get_db_engine()

ticker_list = get_ticker_list()



class StockTickerProducer:
    def __init__(self, bootstrap_servers='kafka:9092', 
                 client_id='producer-nap-batch', 
                 topic_name = "stock_list", 
                 update_conf: dict[str, str | int | float | bool] | None = None):
        
        self.client_id = client_id
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name

        # 1. Cấu hình Producer
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': client_id,
            'enable.idempotence': True, # Đảm bảo tin không bị duplicate
        }
        
        if update_conf:
            self.conf.update(update_conf)
            
        logger.debug(f"Cấu hình Producer: {self.conf}")
        
        # 2. Khởi tạo Resource
        self.producer = Producer(self.conf)
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # hàm kiểm tra và tạo topic 
    def check_and_create_topic(self, topic_name):
        metadata = self.admin_client.list_topics()
        logger.debug(f"Các topic hiện có: {list(metadata.topics.keys())}", extra = {'client_id': self.conf['client.id']} )
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' đã tồn tại", extra = {'client_id': self.conf['client.id']} )
        else:
            new_topic = NewTopic(topic=topic_name, 
                                num_partitions=3, 
                                replication_factor=1,
                                config = {
                                'cleanup.policy': 'compact,delete',
                                'segment.ms': '3600000',
                                'segment.bytes': '104857600',
                                'min.cleanable.dirty.ratio': '0.5',
                                'retention.ms': '86400000',
                                'delete.retention.ms': '86400000',     
                            }
                            )
            logger.info(f"Topic '{self.topic_name}' chưa tồn tại, đã tạo mới" )
            self.admin_client.create_topics([new_topic])




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
            