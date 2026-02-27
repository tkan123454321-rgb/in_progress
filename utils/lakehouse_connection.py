
import os
from pyiceberg.catalog import load_catalog
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from utils.logger_config import setup_logger
import sys
from confluent_kafka.admin import AdminClient, NewTopic # pyright: ignore[reportPrivateImportUsage]
logger = setup_logger(component="utils")



class LakeHouseClient:
    def __init__(self):   
                  
        self.access_key = os.getenv('MINIO_USER')
        self.secret_key = os.getenv('MINIO_PASSWORD')
        self.endpoint_url = "http://minio:9000" 
        self.bucket_name = "financial-data-lake"

        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=self.endpoint_url,         
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version='s3v4'), 
            region_name='us-east-1'             
        )
        
        self._ensure_bucket_exists()
        
        self.catalog = load_catalog(
            "nessie_catalog",
            **{"type": "rest",
                "uri": "http://nessie:19120/iceberg",  
                "warehouse": f"s3://{self.bucket_name}/",  
                "s3.endpoint": self.endpoint_url,  
                "s3.access-key-id": self.access_key,  
                "s3.secret-access-key": self.secret_key,  
                "s3.region": "us-east-1",  
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }
        )

        self._ensure_medallion_layers()
    
    def _ensure_medallion_layers(self):
        layers = ["bronze", "silver", "gold"]
        for layer in layers:
            try:
                self.catalog.create_namespace_if_not_exists(layer)
                logger.info(f"Namespace '{layer}' is ready.")
            except Exception as e:
                logger.error(f" Error creating namespace '{layer}': {e}", exc_info=True)
                sys.exit(1)


    def _ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        
        except ClientError as e:
            error_code = e.response.get("Error", {})['Code']
            if error_code == '404':
                logger.warning(f"Bucket '{self.bucket_name}' is not existing. Creating...")
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"[S3] created successfully {self.bucket_name}")
                except ClientError as create_err:
                    logger.error(f"[Error] can not create bucket: {create_err}")
                    raise create_err
            else:
                logger.error(f"error raised during bucket check: {e}", exc_info=True)
                raise e



def check_and_create_topic(topic_name='default',bootstrap_servers='kafka:9092'):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    try:
        metadata = admin_client.list_topics()
        logger.debug(f"Các topic hiện có: {list(metadata.topics.keys())}")
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' đã tồn tại")
            return True
        else:
            new_topic = NewTopic(topic=topic_name, 
                                num_partitions=3, 
                                replication_factor=1,
                                config = {
                                'cleanup.policy': 'compact,delete', 
                                'segment.ms': '3600000', # Thời gian tối đa của một segment (1 giờ)
                                'segment.bytes': '104857600', # Kích thước tối đa của một segment (100MB)
                                'min.cleanable.dirty.ratio': '0.5', # Tỷ lệ dữ liệu "rác" (dirty) đạt 50% thì Kafka mới bắt đầu quá trình nén (compaction)
                                'retention.ms': '86400000', # Thời gian lưu trữ tối đa
                                'delete.retention.ms': '86400000',     # Thời gian giữ lại dấu vết (tombstone) sau khi message đã bị xóa nén
                            }
                            )
            future = admin_client.create_topics([new_topic])
            for topic, f in future.items():
                try:
                    f.result()  # Block until the topic is created
                    logger.info(f"Topic '{topic}' created successfully.")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}", exc_info=True)
                    return False
    except Exception as e:
        logger.error(f"Không thể kết nối đến Kafka tại {bootstrap_servers}: {e}", exc_info=True)
        return False