import time
from utils.logger_config import setup_logger
from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import socket
import json
from load.data_lake_client import DatalakeClient
import sys

logger = setup_logger(component="load")
boostrap_servers='kafka:29092'


class KafkaStockConsumer:
    def __init__(self, 
                 bootstrap_servers=boostrap_servers, 
                 group_id='stock-data-consumer-group', 
                 topic_name="balance_sheet",
                 update_conf: dict[str, str | int | float | bool] | None = None
                 ):
        
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic_name = topic_name
        self.buffer = []
        self._loader = DatalakeClient()
        # Cấu hình Consumer
        pid = os.getpid()
        hostname = socket.gethostname()
        client_id = f"stock-consumer-{hostname}-{pid}"
        self.conf = {'bootstrap.servers': bootstrap_servers,
                'client.id': client_id,
                'group.id': 'stock_list_v1',
                'max.poll.interval.ms': 350000,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest',
                }
        if update_conf:
            self.conf.update(update_conf)
            logger.debug(f"Consumer configuration: {self.conf}")
        
        # Tạo Consumer instance
        self.consumer = Consumer(self.conf)
        logger.info(f"✅ Kafka Consumer khởi tạo thành công với client_id={client_id}, group_id={self.group_id}")
        

    
    def consume_batch_messages(self):
        self.consumer.subscribe(topics=[self.topic_name])
        idle_count = 0
        BATCH_SIZE = 20
        FLUSH_INTERVAL = 5
        last_flush_time = time.time()
        try:
            while True:
                current_time = time.time()
                if self.buffer and (current_time - last_flush_time > FLUSH_INTERVAL):
                    logger.info(f"⏰ Hết giờ chờ ({FLUSH_INTERVAL}s). Đẩy {len(self.buffer)} tin tồn kho...")
                    self._flush_and_commit()
                    last_flush_time = time.time() # Reset đồng hồ
                    break
                    
                if len(self.buffer) >= BATCH_SIZE:
                    logger.warning(f"⚠️ Buffer đang đầy ({len(self.buffer)}). Thử đẩy lại...")
                    self._flush_and_commit()
                    last_flush_time = time.time() # Reset đồng hồ
                    if self.buffer: 
                        time.sleep(5) 
                        continue
                try:
                    messages = self.consumer.consume(num_messages = 10, timeout=1.0)
                except RuntimeError as e:
                    logger.error(f"Lỗi runtime khi tiêu thụ tin nhắn: {e}")
                    sys.exit(1)
                if not messages:
                    idle_count += 1
                    time.sleep(1)
                    logger.debug(f"⏳ Không có hàng... Đếm ngược: {idle_count}/10")
                    if idle_count >= 10:
                        logger.info("🛑 Đã đợi 10 giây mà không có tin mới. Dừng Consumer!")
                        break # Thoát khỏi vòng lặp While True -> Nhảy xuống Finally
                    continue
                for msg in messages:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.info(f"Đã đọc hết phân vùng {msg.partition()} của topic {msg.topic()}")
                        elif msg.error().retriable():
                            logger.warning(f"Lỗi tạm thời khi tiêu thụ tin nhắn: {msg.error().str()}")
                            continue
                        elif msg.error().fatal():
                            logger.critical(f"Lỗi nghiêm trọng khi tiêu thụ tin nhắn: {msg.error().str()}")
                            raise KafkaException(msg.error())
                        else:
                            logger.error(f"Lỗi không xác định khi tiêu thụ tin nhắn: {msg.error().str()}")
                            raise KafkaException(msg.error())
                    try:
                        record = json.loads(msg.value().decode('utf-8'))
                        self.buffer.append(record)
                        logger.info(f"đẩy thàng công {msg.key()}|partition {msg.partition()}| offset {msg.offset()}| topic {msg.topic()} vào buffer")
                    except json.JSONDecodeError as e:
                        logger.error(f"Lỗi giải mã JSON cho tin nhắn tại offset {msg.offset()} | topic {msg.topic()}: {msg.key()}: {e}")
                        continue
                    except UnicodeDecodeError as e:
                        logger.error(f"❌ Lỗi Font/Mã hóa: {e}")
                        continue
                    
                    if len(self.buffer) >= BATCH_SIZE:
                        self._flush_and_commit()
                        last_flush_time = time.time() # Reset đồng hồ
        except KafkaException as e:
            logger.critical(f"Lỗi Kafka nghiêm trọng: {e}", exc_info=True)
            sys.exit(1)            
        except KeyboardInterrupt:
            logger.warning("Nhận lệnh dừng từ bàn phím (KeyboardInterrupt). Dừng tiêu thụ tin nhắn.")
            raise
        finally:
            if self.buffer:
                self._flush_and_commit()
                time.sleep(1)
            else:
                logger.info("Không còn tin nhắn nào trong buffer.")
            self.consumer.close()
            
       
    def _flush_and_commit(self):
        if not self.buffer:
            return
        logger.info(f"Buffer đạt {len(self.buffer)} tin. Bắt đầu đẩy MinIO...")
        success = self._loader._minio_put_object(self.buffer)
        if success:
            try:
            
                logger.info(f"Đẩy dữ liệu lên minio thành công Bắt đầu làm sạch buffer")
                self.consumer.commit(asynchronous=False)
                self.buffer = []
                
            except KafkaException as e:
                error = e.args[0]
                if error.retriable():
                    logger.warning(f"Lỗi {error}, sẽ thử lại commit offsets.")
                else:
                    logger.error(f"Lỗi khi commit offsets sau khi đẩy dữ liệu: {error}", exc_info=True)
                    raise e
            
    