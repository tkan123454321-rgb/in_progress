from typing_extensions import Literal
from schema.producer_schema import BaseMetadata
import polars as pl
from time import time
from dotenv import load_dotenv
from utils.logger_config import setup_logger
from pyiceberg.exceptions import NoSuchTableError
import pyarrow as pa
from pyarrow import Table
from utils.lakehouse_client import LakeHouseClient
from schema.producer_schema import OriginalTickerList
logger = setup_logger(component = "load")
load_dotenv()


class LakehouseLoader(LakeHouseClient):
    """
    Class chuyên dụng để GHI (Load/Ingest) dữ liệu vào Lakehouse.
    """
    def __init__(self) -> None:
        super().__init__()
           
    def _put_lakehouse(
        self, 
        config: BaseMetadata,  # 🎯 Nhận trực tiếp đối tượng đã khởi tạo
        arrow_table: pa.Table, 
        mode: Literal["append", "overwrite"] = "append"
    ) -> bool:

        # 1. Kiểm tra kiểu dữ liệu (Dùng TypeError chuẩn xác hơn ValueError)
        if not isinstance(arrow_table, pa.Table):
            logger.error(f"❌ Input không phải PyArrow Table, nhận được: {type(arrow_table)}")
            raise TypeError("Input bắt buộc phải là PyArrow Table")

        table_name = f"bronze.{config.topic}"

        try:
            # 2. Bảng ĐÃ TỒN TẠI -> Load, Đồng bộ Schema và Ghi theo Mode
            table = self.catalog.load_table(table_name)
            
            with table.update_schema() as update:
                update.union_by_name(arrow_table.schema)
                
            if mode == "overwrite":
                table.overwrite(arrow_table)
            else:
                table.append(arrow_table)
                
            logger.debug(f"🧊 Đã cập nhật Schema và ghi ({mode}) thành công vào {table_name}")
            return True

        except NoSuchTableError:
            # 3. Bảng CHƯA TỒN TẠI -> Tạo mới
            # Lưu ý: Khi tạo bảng mới thì luôn luôn dùng append để đẩy data mẻ đầu tiên vào
            table = self.catalog.create_table(
                identifier=table_name, 
                schema=config.iceberg_schema,
                partition_spec=config.iceberg_partition_spec
                )
            table.append(arrow_table)
            
            logger.info(f"🧊 Đã tạo mới và ghi mẻ đầu tiên thành công vào {table_name}")
            return True


    def _put_original_ticker_list(self, model_cls: type[OriginalTickerList]):
        config = model_cls() 
        arrow_table = config._build_arrow_payload_lazy()
        self._put_lakehouse(
            config=config,
            arrow_table = arrow_table,
            mode="overwrite")