import time
import requests
from zoneinfo import ZoneInfo
from utils.exception import RetryableAPIError
from utils.logger_config import setup_logger
from confluent_kafka import Consumer, KafkaError, KafkaException, error, Message, TopicPartition
import os
import socket
import json
from load.lakehouse_loader import LakeHouseClient
from utils.kafka_client import KafkaClient
import sys
import uuid
from datetime import datetime
from typing import Any, Callable, Iterable, ClassVar, Sequence
from polars.exceptions import PolarsError
import polars as pl
import pyarrow as pa
from contextlib import contextmanager

logger = setup_logger(component="load")


class KafkaStockConsumer(KafkaClient):
    CONSUMER_CONFIG: ClassVar[dict[str, Any]] = {
                'max.poll.interval.ms': 350000,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest' }
    MESSAGE_SIZE: ClassVar[int] = 10
    POLL_TIMEOUT: ClassVar[float] = 3.0
    MAX_EMPTY_POLLS: ClassVar[int] = 3
        
    def __init__(self, 
                 topic_name='default',
                 update_conf: dict[str, str | int | float | bool] | None = None
                 ):

        self.topic_name = topic_name
        self._loader = LakeHouseClient()
        # Cấu hình Consumer
        self.group_id = f"consumer-{topic_name}-group"
        pid = os.getpid()
        hostname = socket.gethostname()
        self.client_id = f"stock-consumer-{hostname}-{pid}"
        
        self.conf = {
            'bootstrap.servers': self.DEFAULT_BOOTSTRAP,
            'client.id': self.client_id,
            'group.id': self.group_id,
            ** self.CONSUMER_CONFIG
                }
        if update_conf:
            self.conf.update(update_conf)
            logger.debug(f"Consumer configuration: {self.conf}")
        
        # Tạo Consumer instance
        self.consumer = Consumer(self.conf)
        logger.info(f"✅ Kafka Consumer khởi tạo thành công với client_id={self.client_id}, group_id={self.group_id}")
        
  

    def consume_message(self) -> Iterable[dict[str, Any]]:
        EMPTY_POLL = 0
        self.consumer.subscribe(topics=[self.topic_name])
        logger.info(f"🎧 Đã subscribe topic '{self.topic_name}'. Đang chờ tin...")

        while True:
            # Lấy tin nhắn (tối đa 25 tin mỗi nhịp)
            try:
                messages: Sequence[Message | None] = self.consumer.consume(num_messages=self.MESSAGE_SIZE, timeout=self.POLL_TIMEOUT)
                
                if not messages:
                    EMPTY_POLL += 1
                    logger.info(f"🏁 Không còn tin nhắn nào trên Kafka (Timeout {self.POLL_TIMEOUT}s), đợi lần {EMPTY_POLL}")
                    if EMPTY_POLL >= self.MAX_EMPTY_POLLS:
                        logger.info("⏳ Đã liên tiếp 3 lần poll trống. Giả sử đã hết tin, sẽ đóng Consumer để dừng quá trình nạp.")
                        break
                    continue
                EMPTY_POLL = 0
                for msg in messages:
                    # Bỏ qua rác từ Kafka
                    msg_error = msg.error()
                    if msg_error:
                        # PARTITION_EOF không phải lỗi, nó chỉ báo hiệu đã đọc đến đáy của partition
                        if msg_error.code() == KafkaError.PARTITION_EOF: # type: ignore
                            logger.debug(f"Đã đọc hết partition: {msg.topic()} [{msg.partition()}]")
                        else:
                            # Lỗi mạng chập chờn của tin nhắn này (bỏ qua để đọc tin tiếp theo)
                            logger.error(f"❌ Lỗi Message: {msg_error.str()} | Code: {msg_error.code()}")
                        continue # Bỏ qua phần dưới, quay lại vòng lặp for
                    
                    # --- XỬ LÝ DỮ LIỆU BÌNH THƯỜNG ---
                    if msg.value() is None:
                        continue
                        
                    try:
                        yield json.loads(msg.value().decode('utf-8')) # type: ignore
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(f"❌ Lỗi giải mã dữ liệu: {e}")
                        continue
                    
            except KafkaException as e:
                # Lỗi này văng ra khi hệ thống Kafka có biến lớn (như Broker sập toàn tập)
                logger.error(f"❌ Lỗi hệ thống Kafka: {e}")
                raise e

    
    
    def _flush_and_commit(self,buffer: list[dict[str, Any]]) -> None:
        # 1. Nếu giỏ trống thì không làm gì cả
        if not buffer:
            return
            
        try:
            topic_partitions: list[TopicPartition] = self.consumer.commit(asynchronous=False)
            logger.info(f"✅ Đã đẩy MinIO và Commit Kafka thành công | {topic_partitions}")
            
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError._NO_OFFSET: # type: ignore
                logger.warning("⏩ Không có offset mới để commit (có thể do mẻ lẻ). Bỏ qua an toàn.")
            else:
                # Nếu là lỗi khác (như mất mạng, broker sập) thì văng lỗi ra ngoài!
                logger.error(f"❌ Lỗi chí mạng khi commit offsets: {err}")
                raise e
        finally:
            buffer.clear()
            logger.info("🧹 Đã xóa sạch buffer sau khi cố gắng commit.")
    
    @staticmethod
    def _process_single_message(request_session: requests.Session, msg: dict[str, Any], transform_callable: Callable[[dict[str, Any], Any], list]) -> list | None:
        """
        Consumer thuần túy: Hứng Kafka -> Gọi API -> Gắn thêm ID/Time/data
        """
        ticker = msg.get("ticker","unknown")
        url = msg.get("url")
        if not url:
            logger.warning(f"⚠️ Tin nhắn thiếu URL: {msg} | Ticker: {ticker}")
            return None
        try:
            time.sleep(0.1)
            response = request_session.get(url, timeout=10)

            response.raise_for_status()

            api_data = response.json()
            
            if not api_data:
                logger.info(f"Ticker {ticker} không có dữ liệu cho url {url}. Bỏ qua.")
                return None
            else:
                logger.info(f"✅ API trả về dữ liệu cho ticker {ticker}: {url}. Đang xử lý transform...")
            
            return transform_callable(msg, api_data)
        except (requests.RequestException, json.JSONDecodeError) as e:
            raise RetryableAPIError(ticker=ticker, reason=e, message_id=msg.get("message_id")) from e


    @classmethod
    @contextmanager
    def managed(cls, topic_name: str, update_conf: dict[str, Any] | None = None):
        """
        Context Manager cho Consumer. Đảm bảo luôn close() để rời Consumer Group an toàn.
        """
        logger.info("🔌 [Kafka] Khởi tạo và mở kết nối Consumer...")
        consumer_instance = cls(topic_name=topic_name, update_conf=update_conf)
        
        try:
            yield consumer_instance
        finally:
            logger.info("🔒 Đang đóng kết nối Consumer...")
            try:
                # Lệnh close() này cực kỳ quan trọng của Confluent Kafka
                # Nó sẽ báo với Broker là "tôi nghỉ chơi", để Broker chia lại partition cho máy khác
                consumer_instance.consumer.close()
                logger.info("✅ Đã đóng Consumer an toàn.")
            except Exception as e:
                logger.error(f"❌ Lỗi khi đóng Consumer: {e}")