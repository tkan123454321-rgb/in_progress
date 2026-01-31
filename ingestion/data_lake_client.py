from logging import log
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
from botocore.config import Config
from utils.logger_config import setup_logger

logger = setup_logger(component = "load")
load_dotenv()


class DatalakeClient:
    def __init__(self, 
                 endpoint_url = "http://minio:9000", 
                 access_key = os.getenv('MINIO_USER'), 
                 secret_key = os.getenv('MINIO_PASSWORD'), 
                 bucket_name = "financial-data-lake"):
       
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name

        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=endpoint_url,         
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'), 
            region_name='us-east-1'             
        )
        self._ensure_bucket_exists()
     
    def _ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ [S3] Bucket '{self.bucket_name}' đã tồn tại.")
        
        except ClientError as e:
            error_code = e.response.get("Error", {})['Code']
            if error_code == '404':
                logger.warning(f"Bucket '{self.bucket_name}' is not existing. Creating...")
                try:
                    # 2. Gọi hàm create_bucket như trong hình ông thấy
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ [S3] created successfully {self.bucket_name}")
                except ClientError as create_err:
                    logger.error(f"[Error] can not create bucket: {create_err}")
                    raise create_err
            else:
                logger.error(f"error raised during bucket check: {e}", exc_info=True)
                raise e

            
                
    