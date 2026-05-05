"""
Serving Layer Data Loader.

This module orchestrates the movement of heavily transformed data (OBT - One Big Table)
from the analytical Lakehouse (Trino) to the operational serving database (PostgreSQL),
as well as exporting static assets for the frontend UI.
"""

from common.clients.lakehouse_client import LakeHouseClient
from pathlib import Path
from common.clients.postgres_client import PostgresClient
from common.core.logger_config import setup_logger
from common.clients.postgres_client import map_trino_to_pg_type
from typing import Tuple

logger = setup_logger(component="load")


class WebServingLoader:
    """
    Handles the synchronization of analytical data to the web serving layer.
    Implements zero-downtime deployment patterns to ensure the frontend
    never queries an empty or partially loaded table.
    """

    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        """
        Initializes the loader by injecting pre-configured database clients.
        Acts purely as an orchestrator for the existing connections.
        """
        self.pg_conn = pg_client.get_db_connection(db_name="ops_db")
        self.trino_conn = lake_client._get_trino_connection()
        self.catalog = lake_client.catalog

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Delegates connection teardown to the underlying client libraries."""
        if self.pg_conn:
            self.pg_conn.__exit__(exc_type, exc_val, exc_tb)
            logger.info("Closed connection to PostgreSQL.")
        if self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
            logger.info("Closed connection to Trino.")

    def _extract_trino_payload(self) -> Tuple[str, str, list[tuple]]:
        """
        Extracts both the schema and the raw data from the Lakehouse's OBT layer.

        Returns:
            Tuple containing:
            - final_sql_columns: String formatted for CREATE TABLE statement.
            - copy_columns_str: String formatted for COPY statement.
            - rows: The actual data records.
        """
        logger.info(
            "Extracting schema definition and data payload from Lakehouse (obt.obt_web)."
        )
        with self.trino_conn.cursor() as cur:
            # STEP 1: Extract Schema definitions
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'obt_web'
                ORDER BY ordinal_position
            """)

            key_values = []
            col_names = []
            for col in cur.fetchall():
                col_name = col[0]
                data_type = map_trino_to_pg_type(col[1])

                key_values.append(f'"{col_name}" {data_type}')
                col_names.append(
                    f'"{col_name}"'
                )  # Bọc nháy kép luôn để lát ném vào COPY

            final_sql_columns = ", ".join(key_values)
            copy_columns_str = ", ".join(
                col_names
            )  # Ra dạng: "ticker", "qmj_score", ...

            # STEP 2: Extract Data records
            cur.execute("SELECT * FROM obt.obt_web")
            rows = cur.fetchall()

        return (final_sql_columns, copy_columns_str, rows)

    def sync_obt_to_postgres(self):
        """
        Main execution method. Synchronizes data from Trino to PostgreSQL
        using a Zero-Downtime swap pattern.
        """
        try:
            # STEP 1: Fetch payload from Lakehouse
            final_sql_columns, copy_columns_str, rows = self._extract_trino_payload()

            if not rows:
                logger.warning(
                    "Lakehouse table is empty. halting synchronization process."
                )
                return

            logger.info("Initiating PostgreSQL synchronization for 'web.web_obt'.")
            # STEP 2: Execute safe transaction block
            with self.pg_conn.transaction():
                with self.pg_conn.cursor() as pg_cur:
                    # 2.1. Build temporary table
                    pg_cur.execute("DROP TABLE IF EXISTS web.web_obt_temp")
                    pg_cur.execute(
                        f"CREATE TABLE web.web_obt_temp ({final_sql_columns})"  # type: ignore
                    )
                    # 2.2. Bulk insert via COPY (High-performance loading)
                    logger.info(f"Executing bulk COPY for {len(rows)} records.")
                    copy_query = (
                        f"COPY web.web_obt_temp ({copy_columns_str}) FROM STDIN"
                    )

                    with pg_cur.copy(copy_query) as copy_operation:  # type: ignore
                        for row in rows:
                            copy_operation.write_row(row)

                    # 2.3. Build indexes on the temporary table BEFORE swapping
                    self._create_optimal_indexes(pg_cur)

                    # 2.4. Zero-Downtime Table Swap
                    logger.info("Performing zero-downtime table swap.")
                    pg_cur.execute("DROP TABLE IF EXISTS web.web_obt_old")

                    # Archive the current active table
                    pg_cur.execute("""
                        DO $$
                        BEGIN
                            IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'web' AND tablename = 'web_obt') THEN
                                ALTER TABLE web.web_obt RENAME TO web_obt_old;
                            END IF;
                        END $$;
                    """)

                    # Promote the temporary table to active status
                    pg_cur.execute("ALTER TABLE web.web_obt_temp RENAME TO web_obt")

            logger.info("Synchronization complete. Data is now live in PostgreSQL.")

        except Exception as e:
            logger.error(f"❌ Lỗi rồi: {e}")
            raise e

    def _create_optimal_indexes(self, pg_cur):
        logger.info("Constructing analytical indexes on the temporary table.")
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("ticker")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("year", "quarter")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("qmj_rank")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("z_value_recent")')
        pg_cur.execute('CREATE INDEX ON web.web_obt_temp ("z_momentum_recent")')
        pg_cur.execute(
            'CREATE INDEX ON web.web_obt_temp ("year", "quarter", "qmj_rank")'
        )
        logger.info("Index construction complete.")

    def export_web_data(self):
        """
        Exports the finalized OBT table directly from the Iceberg catalog
        to a local CSV file for static frontend serving.
        """
        try:
            table_path = "gold.obt_web"
            logger.info(f"Initiating data export from Lakehouse table: '{table_path}'.")

            table = self.catalog.load_table(table_path)

            df = table.scan().to_polars()

            project_root = Path(__file__).resolve().parents[1]
            output_path = project_root / "web_ui" / "data_qmj.csv"
            df.write_csv(output_path)

            logger.info(
                f"Successfully exported {df.height} records to '{output_path}'."
            )

        except Exception as e:
            logger.error(f"Failed to export web data. Error: {e}", exc_info=True)
            raise e
