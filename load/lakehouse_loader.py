from typing_extensions import Literal
from schema.producer_schema import BaseMetadata
from dotenv import load_dotenv
from common.core.logger_config import setup_logger
from pyiceberg.exceptions import NoSuchTableError
import pyarrow as pa
from common.clients.lakehouse_client import LakeHouseClient

logger = setup_logger(component="load")
load_dotenv()


class LakehouseLoader(LakeHouseClient):
    """
    Specialized client for loading data into the Lakehouse.

    Inherits infrastructure connectivity from LakeHouseClient and provides
    methods to physically write PyArrow tables into Iceberg format on MinIO.
    """

    def __init__(self) -> None:
        super().__init__()

    def _put_lakehouse(
        self,
        config: BaseMetadata,
        arrow_table: pa.Table,
        mode: Literal["append", "overwrite"] = "append",
    ) -> bool:
        """
        Writes a PyArrow table to an Iceberg table in the Bronze layer.

        Supports automatic schema evolution (union by name) and handles
        the initial bootstrapping if the table does not exist.

        Args:
            config (BaseMetadata): config (BaseMetadata): The instance of data classes located in `schema.producer_schema`.
            arrow_table (pa.Table): The in-memory PyArrow table to be written.
            mode (Literal["append", "overwrite"], optional): Write mode. Defaults to "append".

        Returns:
            bool: True if the write operation was successful.
        """

        # STEP 1: Input Validation (Fail-fast)
        # Ensure the payload is strictly a PyArrow Table
        if not isinstance(arrow_table, pa.Table):
            error_msg = f"Invalid input type. Expected PyArrow Table, received: {type(arrow_table)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        table_name = f"bronze.{config.bronze_layer_name}"  # type: ignore

        try:
            # STEP 2: Existing Table Workflow
            # If the table exists, we load it, evolve the schema safely, and write the data.
            table = self.catalog.load_table(table_name)
            # Auto-schema evolution: Merge the new incoming schema with the existing Iceberg schema.
            # This safely handles newly added columns from upstream APIs.
            with table.update_schema() as update:
                update.union_by_name(arrow_table.schema)

            if mode == "overwrite":
                table.overwrite(arrow_table)
            else:
                table.append(arrow_table)

            logger.debug(
                f"Successfully synced schema and wrote ({mode}) data to '{table_name}'."
            )
            return True

        except NoSuchTableError:
            # STEP 3: Initial Table Bootstrapping
            # If the table doesn't exist yet, we create it using the strict schema
            # and partition specs defined in the configuration model.
            table = self.catalog.create_table(
                identifier=table_name,
                schema=config.iceberg_schema,
                partition_spec=config.iceberg_partition_spec,
            )
            table.append(arrow_table)

            logger.info(
                f"Successfully created table and ingested the first batch into '{table_name}'."
            )
            return True
