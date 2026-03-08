import os
import logging
from typing import ClassVar, Dict, Optional
from confluent_kafka.admin import AdminClient, NewTopic # type: ignore
from utils.logger_config import setup_logger

logger = setup_logger(component="utils")

class KafkaClient:
    """
    Class quản lý các tác vụ Admin của Kafka (Tạo, Xóa, Kiểm tra Topic).
    """
    # 1. Khai báo Class Variables cho cấu hình mặc định
    DEFAULT_BOOTSTRAP: ClassVar[str] = 'kafka:29092'
    DEFAULT_PARTITIONS: ClassVar[int] = 3
    DEFAULT_REPLICATION: ClassVar[int] = 1
    
    # Gom toàn bộ config rườm rà của Topic lên đây cho sạch ruột hàm
    TOPIC_CONFIG: ClassVar[Dict[str, str]] = {
        'cleanup.policy': 'compact,delete', 
        'segment.ms': '3600000',            # Thời gian tối đa của một segment (1 giờ)
        'segment.bytes': '104857600',       # Kích thước tối đa của một segment (100MB)
        'min.cleanable.dirty.ratio': '0.5', # Tỷ lệ rác đạt 50% thì nén
        'retention.ms': '86400000',         # Thời gian lưu tối đa (24h)
        'delete.retention.ms': '86400000',  # Thời gian giữ tombstone (24h)
    }
    
    def __init__(self, topic_name: str) -> None:
        """
        Khởi tạo là tự động kết nối Kafka Admin luôn.
        Nếu không truyền bootstrap_servers, tự động lấy cấu hình mặc định.
        """
        self.admin_client = AdminClient({'bootstrap.servers': self.DEFAULT_BOOTSTRAP})
        self._check_and_create_topic(topic_name)  # Kiểm tra và tạo topic mặc định ngay khi khởi tạo
        
        
    def _check_and_create_topic(self, topic_name: str):
        try:
            metadata = self.admin_client.list_topics()
            logger.debug(f"Các topic hiện có: {list(metadata.topics.keys())}")
            if topic_name in metadata.topics:
                logger.info(f"Topic '{topic_name}' đã tồn tại")
                return True
            else:
                new_topic = NewTopic(topic=topic_name, 
                                    num_partitions=self.DEFAULT_PARTITIONS, 
                                    replication_factor=self.DEFAULT_REPLICATION,
                                    config = self.TOPIC_CONFIG
                                )
                future = self.admin_client.create_topics([new_topic])
                for topic, f in future.items():
                    try:
                        f.result()  # Block until the topic is created
                        logger.info(f"Topic '{topic}' created successfully.")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to create topic '{topic}': {e}", exc_info=True)
                        return False
        except Exception as e:
            logger.error(f"Không thể kết nối đến Kafka tại {self.DEFAULT_BOOTSTRAP}: {e}", exc_info=True)
            return False