import trino.dbapi
import os
from pyiceberg.catalog import load_catalog
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from typing import ClassVar
from common.core.logger_config import setup_logger
import sys
from confluent_kafka.admin import AdminClient, NewTopic # pyright: ignore[reportPrivateImportUsage]
from pyiceberg.exceptions import NoSuchTableError
from typing import Sequence, Generator, Literal

logger = setup_logger(component="infrastructure")



class LakeHouseClient:
    """
    Manages connection and infrastructure for the Data Lakehouse ecosystem.
    
    This client orchestrates 3 components:
    1. Storage: MinIO.
    2. Catalog: Nessie.
    3. Compute: Trino.
    """
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
        "intermediate",
        "gold"
    ]
    
    
    def __init__(self) -> None:
        """
        Initializes clients for S3 and Nessie, and ensures infrastructure 
        (buckets and namespaces) is properly configured.
        """  
        # Initialize MinIO (S3) Client
        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=self.ENDPOINT_URL,         
            aws_access_key_id=self._ACCESS_KEY,
            aws_secret_access_key=self._SECRET_KEY,
            config=Config(signature_version='s3v4'), 
            region_name=self.REGION             
        )
        # STEP 1: Ensure physical storage exists BEFORE configuring logical namespaces
        self._ensure_bucket_exists()
        # Initialize Nessie Catalog (Iceberg)
        self.catalog = load_catalog(
            "nessie_catalog",
            **{"type": "rest",
                "uri": "http://nessie:19120/iceberg",
                "snapshot-loading-mode": "refs",
                "warehouse": f"s3://{self.BUCKET_NAME}/",  
                "s3.endpoint": self.ENDPOINT_URL,  
                "s3.access-key-id": self._ACCESS_KEY,  
                "s3.secret-access-key": self._SECRET_KEY,  
                "s3.region": self.REGION,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }
        )
        # 4. Create required namespaces (bronze, silver, gold, etc.) 
        self._ensure_medallion_layers()

    
    @staticmethod
    def _get_trino_connection() -> trino.dbapi.Connection:
        """
        Establishes a connection to the Trino.
        
        Returns:
            trino.dbapi.Connection.
        """
        return trino.dbapi.connect(
            host="trino",
            port=8080,
            user="admin",
            catalog="lakehouse_main")
        

    def _ensure_medallion_layers(self) -> None:
        """
        checks the existence of namespaces (schemas) required
        (Bronze, Silver, Gold, etc.) within the Iceberg catalog. If any are missed, it creates them.
        
        Raises:
            RuntimeError: If namespace provisioning fails.
        """
        for layer in self.REQUIRED_SCHEMAS:
            try:
                self.catalog.create_namespace_if_not_exists(layer)
            except Exception as e:
                logger.error(f" Error creating namespace '{layer}': {e}", exc_info=True)
                sys.exit(1)


    def _ensure_bucket_exists(self) -> None:
        """
        Checks if the MinIO bucket exists. If not, creates it.
        """
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

     
   
    
        

        
   
                        
        




