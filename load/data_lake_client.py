
from io import BytesIO
from time import time
from dotenv import load_dotenv
from utils.logger_config import setup_logger
from pyiceberg.exceptions import NoSuchTableError
import pyarrow as pa
from pyarrow import Table
from utils.lakehouse_connection import LakeHouseClient
logger = setup_logger(component = "load")
load_dotenv()


class LakehouseLoader:
    """
    Class chuyên dụng để GHI (Load/Ingest) dữ liệu vào Lakehouse.
    """
    def __init__(self):
        # Khởi tạo hạ tầng
        self.infra = LakeHouseClient()
        self.catalog = self.infra.catalog
        self.s3_client = self.infra.s3_client
        self.bucket_name = self.infra.bucket_name
    
        
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