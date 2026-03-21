import trino.dbapi
import os
from pyiceberg.catalog import load_catalog
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from schema.producer_schema import ClassVar
from utils.logger_config import setup_logger
import sys
from confluent_kafka.admin import AdminClient, NewTopic # pyright: ignore[reportPrivateImportUsage]
from pyiceberg.exceptions import NoSuchTableError
from typing import Sequence, Generator, Literal

logger = setup_logger(component="utils")



class LakeHouseClient:
    _ACCESS_KEY: ClassVar[str] = os.getenv('MINIO_USER', 'default_user')
    _SECRET_KEY: ClassVar[str] = os.getenv('MINIO_PASSWORD', 'default_pass')
    ENDPOINT_URL: ClassVar[str] = "http://minio:9000"
    BUCKET_NAME: ClassVar[str] = "financial-data-lake"
    REGION: ClassVar[str] = "us-east-1"
    REQUIRED_SCHEMAS: ClassVar[list[str]] = [
        "elementary", 
        "bronze", 
        "staging", 
        "silver", 
        "gold"
    ]
    
    
    def __init__(self) -> None:   
        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=self.ENDPOINT_URL,         
            aws_access_key_id=self._ACCESS_KEY,
            aws_secret_access_key=self._SECRET_KEY,
            config=Config(signature_version='s3v4'), 
            region_name=self.REGION             
        )
        
        self.catalog = load_catalog(
            "nessie_catalog",
            **{"type": "rest",
                "uri": "http://nessie:19120/iceberg/dev",
                "snapshot-loading-mode": "refs",
                "warehouse": f"s3://{self.BUCKET_NAME}/",  
                "s3.endpoint": self.ENDPOINT_URL,  
                "s3.access-key-id": self._ACCESS_KEY,  
                "s3.secret-access-key": self._SECRET_KEY,  
                "s3.region": self.REGION,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }
        )
        self._ensure_medallion_layers()
        self._ensure_bucket_exists()
    
    @staticmethod
    def _get_trino_connection(type: Literal["maintenance", "read"] = "read") -> trino.dbapi.Connection:
        if type == "maintenance":
            return trino.dbapi.connect(
                host="trino",
                port=8080,
                user="admin",
                catalog="lakehouse_main")
        elif type == "read":
            return trino.dbapi.connect(
                host="trino",
                port=8080,
                user="admin",
                catalog="lakehouse_dev")
        else:
            raise ValueError(f"Invalid connection type '{type}' requested.")

    def _ensure_medallion_layers(self) -> None:
        for layer in self.REQUIRED_SCHEMAS:
            try:
                self.catalog.create_namespace_if_not_exists(layer)
            except Exception as e:
                logger.error(f" Error creating namespace '{layer}': {e}", exc_info=True)
                sys.exit(1)


    def _ensure_bucket_exists(self) -> None:
        try:
            self.s3_client.head_bucket(Bucket=self.BUCKET_NAME)
        
        except ClientError as e:
            error_code = e.response.get("Error", {})['Code']
            if error_code == '404':
                logger.warning(f"Bucket '{self.BUCKET_NAME}' is not existing. Creating...")
                try:
                    self.s3_client.create_bucket(Bucket=self.BUCKET_NAME)
                    logger.info(f"[S3] created successfully {self.BUCKET_NAME}")
                except ClientError as create_err:
                    logger.error(f"[Error] can not create bucket: {create_err}")
                    raise create_err
            else:
                logger.error(f"error raised during bucket check: {e}", exc_info=True)
                raise e

     
   
    
        

        
   
                        
        




