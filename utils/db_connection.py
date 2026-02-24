from sqlalchemy import *
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog



def get_catalog():
    catalog = load_catalog(
    "nessie_catalog",
    **{"type": "rest",
        "uri": "http://nessie:19120/iceberg",  
        "warehouse": "s3://financial-data-lake/",  
        "s3.endpoint": "http://minio:9000",  
        "s3.access-key-id": os.getenv("MINIO_USER"),  
        "s3.secret-access-key": os.getenv("MINIO_PASSWORD"),  
        "s3.region": "us-east-1",  
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
    }
)
    return catalog

