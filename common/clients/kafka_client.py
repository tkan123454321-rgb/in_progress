import os
import logging
from typing import ClassVar, Dict, Optional
from confluent_kafka.admin import AdminClient, NewTopic # type: ignore
from common.core.logger_config import setup_logger

logger = setup_logger(component="infrastructure")

class KafkaClient:
    """
    Manages Kafka administrative tasks such as topic validation and creation.
    
    This client ensures that the required infrastructure (topics with specific 
    retention and compaction policies) is firmly in place before any producers 
    or consumers attempt to interact with the message broker.
    """
    # default configs
    DEFAULT_BOOTSTRAP: ClassVar[str] = 'kafka:29092'
    DEFAULT_PARTITIONS: ClassVar[int] = 3
    DEFAULT_REPLICATION: ClassVar[int] = 1
    TOPIC_CONFIG: ClassVar[Dict[str, str]] = {
        'cleanup.policy': 'compact,delete', 
        'segment.ms': '3600000',            # Force roll segments every 1 hour
        'segment.bytes': '104857600',       # Max segment size (100MB)
        'min.cleanable.dirty.ratio': '0.5', # Trigger compaction when 50% is dirty
        'retention.ms': '86400000',         # Delete messages older than 24h
        'delete.retention.ms': '86400000',  # Keep tombstone markers for 24h
    }
    
    def __init__(self, topic_name: str) -> None:
        """
        Initializes the Kafka Admin client and guarantees the target topic exists.

        Args:
            topic_name (str): The name of the Kafka topic to manage.
        """
        self.admin_client = AdminClient({'bootstrap.servers': self.DEFAULT_BOOTSTRAP})
        self._check_and_create_topic(topic_name)  # Kiểm tra và tạo topic mặc định ngay khi khởi tạo
        
        
    def _check_and_create_topic(self, topic_name: str):
        """
        Validates if the topic exists; if not, provisions it with strict configurations.
        """
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