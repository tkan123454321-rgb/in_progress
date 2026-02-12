import datetime
from io import BytesIO
import json
from logging import log
import sys
from time import time
import uuid
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
from botocore.config import Config
from utils.logger_config import setup_logger
import time
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
import pyarrow as pa
from pyarrow import Table
logger = setup_logger(component = "load")
load_dotenv()


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
    
    def _minio_put_object(self, buffer):
        if not buffer:
                logger.info("[Info] No data to upload to MinIO.")
                return True
        try:
            now = datetime.datetime.now()
            unique_id = uuid.uuid4().hex[:8]
            file_name = f"batch_{int(time.time())}_{unique_id}.jsonl"
            file_key = (
                        f"source=fireant/"
                        f"layer=bronze/"
                        f"year={now.year}/"
                        f"month={now.month:02d}/"
                        f"day={now.day:02d}/"
                        f"{file_name}"
                        )
            
            jsonl_string = "\n".join([json.dumps(record, ensure_ascii=False) for record in buffer])
            
            # Đóng gói vào hộp ảo BytesIO
            file_buffer = jsonl_string.encode('utf-8')

            # 4. UPLOAD: Đẩy lên MinIO
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,       
                Body=file_buffer    
            )
            return True
        except ClientError as e:
            logger.error(f"[Error] uploading to MinIO failed: {e}", exc_info=True)
            return False
        
    def _put_raw_dim_company(self, arrow_table, name_table):
        if not isinstance(arrow_table, pa.Table):
            logger.error(f"Expected a PyArrow Table, but got {type(arrow_table)}")
            raise ValueError("Input must be a PyArrow Table")
        table_name = f"bronze.{name_table}"
        try:
            table = self.catalog.load_table(table_name)
            with table.update_schema() as update:
                update.union_by_name(arrow_table.schema)
            table.overwrite(arrow_table)
            logger.info(f"Successfully wrote {table_name} to catalog")
        except NoSuchTableError:
            table = self.catalog.create_table(
                    table_name,
                    schema=arrow_table.schema)
            table.append(arrow_table)
            logger.info(f"Successfully wrote {table_name} to catalog")