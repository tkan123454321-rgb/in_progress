from __future__ import annotations
from common.core.logger_config import setup_logger
from common.clients.postgres_client import PostgresClient
from common.clients.lakehouse_client import LakeHouseClient, NoSuchTableError
from typing import Literal, TYPE_CHECKING
from datetime import datetime, date
import polars as pl
import psycopg
from psycopg import sql
from psycopg.rows import dict_row, DictRow
from typing import ClassVar, Generator, Sequence
from contextlib import contextmanager
import trino.dbapi
from pyiceberg.expressions import EqualTo
from common.core.time_utils import get_target_anchor, get_fallback_year
from zoneinfo import ZoneInfo
if TYPE_CHECKING:
    from schema.producer_schema import BaseMetadata


logger = setup_logger(component="ingest")


class MetadataManager:
    """
    The manager for the ingestion and load state of stock market data 
    (e.g., historical prices, financial statements, fundamental data).
    
    This class guarantees data integrity. It ensures 
    that data is ingested accurately, completely, and without duplicating data period. 
    
    Key responsibilities include:
        - Tracking incremental watermarks (last updated dates) for each of data for each stock ticker.
        - Retrieving and reconciling reference dimension data (active ticker lists) from the Lakehouse.
        - Maintaining ingestion and load states via PostgreSQL database.
        - Cleaning up ingestion states and executing recovery protocols.

    Usage Example:
        with MetadataManager(PostgresClient(), LakeHouseClient()) as manager:
            # Fetch the list of active tickers from the Lakehouse
            active_tickers = manager._get_ticker_list_raw(mode="fundamental")
    """
    DB_NAME : ClassVar[str] = "ops_db"
    
    
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        """
        Initializes the MetadataManager with established database connections.

        Args:
            pg_client (PostgresClient): Client for PostgreSQL operations.
            lake_client (LakeHouseClient): Client for Lakehouse/Trino operations.
        """
        self.pg_conn : psycopg.Connection = pg_client.get_db_connection(db_name=self.DB_NAME)  # type: ignore
        self.trino_conn : trino.dbapi.Connection  = lake_client._get_trino_connection()
        self.catalog = lake_client.catalog
        self.pg_conn_str : str = pg_client._build_conn_str(db_name=self.DB_NAME)
    
    
    
    def _get_ticker_list_raw(self, mode : Literal["fundamental", "other_data", "vnindex"] = "fundamental" ) -> set[str]:
        """
        Retrieves the authoritative list of stock tickers from the Lakehouse based on specific business rules.

        Args:
            mode (Literal): The extraction mode defining which table to query and what business logic applies.
                - 'fundamental': Retrieves the raw, unfiltered reference list of all tickers from the Silver layer. 
                  This serves as the baseline for fetching fundamental data (stages 1 & 2) before any strict 
                  business filtering is applied.
                - 'other_data': Retrieves a curated list of tickers from the Gold layer ('gold_dim_company'). 
                  These companies have passed strict business qualification tests (e.g., 3-month average 
                  trading volume, market capitalization thresholds) and are deemed eligible for deep financial 
                  data extraction.
                - 'vnindex': Returns a static set containing only 'VNINDEX', serving as the reference 
                  for fetching daily market index points.

        Returns:
            set[str]: A set of distinct ticker symbols matching the selected mode.

        Raises:
            ValueError: If an invalid mode is provided.
            NoSuchTableError: If the target Iceberg table does not exist in the catalog.
        """
        try:
            if mode == "fundamental":
                table = "silver.silver_dim_company"
                tbl = self.catalog.load_table(table)
                tbl = tbl.scan(selected_fields=("ticker",)).to_polars()
                result = set(tbl["ticker"].to_list())
                logger.info(f"Successfully fetch {len(result)} tickers")
                return result
            elif mode == "other_data":
                table = "gold.gold_dim_company"
                tbl = self.catalog.load_table(table)
                tbl = tbl.scan(row_filter=EqualTo(term = "status", literal = "qualified"), selected_fields=("ticker",)).to_polars()
                result = set(tbl["ticker"].to_list())
                logger.info(f"Successfully fetch {len(result)} tickers")
                return result
            elif mode == "vnindex":
                return {'VNINDEX'}
            else:
                raise ValueError(f"Invalid mode '{mode}' for fetching ticker list.")
        except NoSuchTableError as e:
            logger.error(f"Table {table} does not exist.")
            raise e
    
    
    
    def log_metadata_to_db(self, table_name: str, batch_id: str, ticker: str, data_type: str)-> None:
        """
        Updates the ingestion state into PostgreSQL.
        This method acts as the core tracking mechanism for the pipeline to:
        1. Prevent Kafka message duplication (ensuring exactly-once message production).
        2. Guarantee data completeness for the downstream consumers before they load 
           data into the Lakehouse.
           
        Note: The pipeline implements the "Claim-Check" pattern. The Kafka messages 
        tracked here do not contain heavy data payloads. Instead, they carry metadata 
        pointers (e.g., source URLs, timestamps). Downstream consumers will read these 
        pointers from Kafka and independently fetch the actual payloads.

        Args:
            table_name (str): The target PostgreSQL state table name.
            batch_id (str): The unique identifier for the current ingestion batch.
            ticker (str): The stock ticker symbol.
            data_type (str): The category of data being ingested.
        """
        query = sql.SQL("""
            INSERT INTO ingestion.{table_name} 
            (batch_id, data_type, ticker, created_time)
            VALUES (%(batch_id)s, %(data_type)s, %(ticker)s, %(created_time)s)
        """).format(table_name=sql.Identifier(table_name))
        
        data = {
            "batch_id": batch_id,
            "data_type": data_type,
            "ticker": ticker,
            "created_time": datetime.now()
        }
        
        try:
            self.pg_conn.execute(query, data) 
            logger.debug(f"Logged metadata for ticker '{ticker}' with batch_id '{batch_id}' and data_type '{data_type}' into Postgres.")
        except psycopg.Error as e:
            logger.error(
                f"Database error during metadata logging. Ticker: '{ticker}', "
                f"SQLState: '{e.sqlstate}', Error: {e}", exc_info=True
            )
            raise e



            
    def cleanup_kafka_state(self, table_name: str, data_type: str) -> None:
        """
        Runs once at the start of the pipeline for each data type.
        Purges Kafka state records SPECIFIC TO THAT DATA TYPE if they are older than 24 hours.
        """
        cleanup_sql = sql.SQL("""
            DELETE FROM ingestion.{table}
            WHERE data_type = %(data_type)s 
              AND created_time < NOW() - INTERVAL '24 HOURS';
        """).format(table=sql.Identifier(table_name))
        

        with self.pg_conn.cursor() as cur:
            cur.execute(cleanup_sql, {'data_type': data_type})
            deleted_rows = cur.rowcount 
            
        # Log kết quả ra màn hình
        if deleted_rows > 0:
            logger.info(f"Purged expired Kafka state records (>24h). Table: '{table_name}', Data Type: '{data_type}', Deleted Rows: {deleted_rows}.")
        else:
            logger.info(f"No expired Kafka state records found. Table: '{table_name}', Data Type: '{data_type}'. State is clean.")
                
        



    def get_missing_tickers(self, table_name: str, tickers_set: set[str], data_type: str):
        """
        Identifies missing tickers for the daily ingestion by substracting the reference list of tickers  
        with the ones currently stored in PostgreSQL database.

        Since expired states (older than 24h) are purged by `cleanup_kafka_state`, 
        the PostgreSQL table only represents tickers that have successfully received a claim-check 
        ticket today. By subtracting the Postgres list from the full list, missing claim-check tickets can be identified. 
        This allows the pipeline to issue catch-up tickets for those tickers.

        Args:
            table_name (str): The PostgreSQL state table tracking today's claim-check metadata.
            tickers_set (set[str]): The reference set of expected tickers (typically generated by the `_get_ticker_list_raw` method).
            data_type (str): The specific data type we are checking (e.g., 'fundamental').

        Returns:
            set[str]: A set of tickers that require catch-up claim-check ticket issuance.
        """
        query = f"SELECT ticker FROM ingestion.{table_name} where data_type = '{data_type}'"
        # Polars with ConnectorX is highly optimized for PostgreSQL reads
        df = pl.read_database_uri(query=query, uri=self.pg_conn_str, engine="connectorx")
        
        if df.is_empty():
            processed_tickers_set = set()
        else:
            processed_tickers_set = set(df["ticker"].to_list())
            
        missing_tickers = tickers_set - processed_tickers_set
        
        if missing_tickers:
            logger.warning(
                f"Missing tickers detected for the last 24h. "
                f"Data Type: '{data_type}', Missing Count: {len(missing_tickers)}. Initiating catch-up phase."
            )
        else:
            logger.info(f"All active tickers have been successfully ingested in the last 24h. Data Type: '{data_type}'.")
            
        return missing_tickers




    def sync_watermark(self, config: BaseMetadata) -> None:
        """
        Keeps the Postgres watermark table in sync with the master ticker list from the Lakehouse.
        
        Before we pull any data, we need to know exactly which tickers are currently active. 
        This method compares the Lakehouse single source of Truth with our Postgres tracking table 
        and handles three scenarios:
        1. New tickers: Inserts them into Postgres with a default baseline start date.
        2. Disappeared tickers: Marks them as 'inactive' -> stop wasting API calls on them.
        3. Returning tickers: Reactivates tickers that previously disappeared but are now back.

        Args:
            config (BaseMetadata): The instance of data classes located in `schema.producer_schema`. 
            
        Raises:
            ValueError: If the Postgres watermark table name is not configured.
        """
        logger.info(f"Starting watermark sync. Reconciling Lakehouse reference list with Postgres watermark table for '{config.data_type}'...")
        if not config.table_watermark_name_postgres:
            raise ValueError(f"Missing Postgres Watermark table name in config for data type '{config.data_type}'.")
        # STEP 1: Fetch the reference list of tickers (based on ticker_list_mode) from the Lakehouse (Gold/Silver layer)
        gold_tickers = self._get_ticker_list_raw(mode= config.ticker_list_mode)

        # STEP 2: Fetch the current state (tickers and their status) from Postgres
        with self.pg_conn.cursor() as cur:
            select_query = sql.SQL("""
                SELECT ticker, ticker_status 
                FROM ingestion.{table}
                WHERE data_type = %(data_type)s
            """).format(table=sql.Identifier(config.table_watermark_name_postgres))
            
            cur.execute(select_query, {'data_type': config.data_type})
            db_data = cur.fetchall()
            
            db_tickers = {row['ticker'] for row in db_data}  # type: ignore
            inactive_db_tickers = {row['ticker'] for row in db_data if row['ticker_status'] == 'inactive'}  # type: ignore

        # STEP 3: Calculate the differences (Deltas) using Python sets
        new_tickers = gold_tickers - db_tickers
        disappeared_tickers = db_tickers - gold_tickers
        reappeared_tickers = (gold_tickers & db_tickers) & inactive_db_tickers
        
        # If everything matches perfectly, we can exit early and save database trips
        if not any([new_tickers, disappeared_tickers, reappeared_tickers]):
            logger.info(f"Watermark is 100% synchronized. No updates needed for '{config.data_type}'.")
            return
        # STEP 4: Apply the calculated updates in a single database transaction
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as cur:
                
                # 4.1. Handle New Tickers
                # If they are new, we set their 'updated_at' to a fallback date (the return from get_fallback_year())
                # so the pipeline knows to fetch their full historical data.
                if new_tickers:
                    fallback_year = get_fallback_year()
                    fallback_time_str = f"{fallback_year}-01-01 00:00:00+00"
                    insert_query = sql.SQL("""
                        INSERT INTO ingestion.{table} 
                        (ticker, data_type, ticker_status, updated_at)
                        VALUES (%(ticker)s, %(data_type)s, 'active', %(fallback_time)s)
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    cur.executemany(insert_query, [{'ticker': t, 'data_type': config.data_type, 'fallback_time': fallback_time_str} for t in new_tickers])
                    logger.info(f"Inserted new tickers. Data Type: '{config.data_type}', Count: {len(new_tickers)}.")

                # 4.2. Handle Disappeared Tickers
                # Tickers that dropped out of the reference list (e.g., delisted or failed business rules) will be marked as 'inactive' in Postgres. 
                # This allows the pipeline to skip them without losing historical state, and also provides an easy way to reactivate them if they come back in the future.
                if disappeared_tickers:
                    deactivate_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET ticker_status = 'inactive'
                        WHERE ticker = %(ticker)s AND data_type = %(data_type)s
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    cur.executemany(deactivate_query, [{'ticker': t, 'data_type': config.data_type} for t in disappeared_tickers])
                    logger.warning(f"Deactivated missing tickers. Data Type: '{config.data_type}', Count: {len(disappeared_tickers)}.")

                # 4.3. Handle Returning Tickers
                # Tickers that were inactive but have now reappeared in the reference list
                if reappeared_tickers:
                    reactivate_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET ticker_status = 'active'
                        WHERE ticker = %(ticker)s AND data_type = %(data_type)s
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    
                    cur.executemany(reactivate_query, [{'ticker': t, 'data_type': config.data_type} for t in reappeared_tickers])
                    logger.info(f"Reactivated returning tickers. Data Type: '{config.data_type}', Count: {len(reappeared_tickers)}.")
                
        logger.info(f"Watermark reconciliation completed successfully. Data Type: '{config.data_type}'.")
        
        
        
        
    def _get_smart_start_date(self, ticker: str, config: BaseMetadata) -> date:
        """
        this helps look up the 'updated_at' timestamp 
        in the Postgres watermark table to identify the pipeline exactly where it left off 
        during the last run. This ensures we only fetch fresh data (incremental load) 
        and avoid dispatching the same historical data twice to the lakehouse

        Args:
            ticker (str): The specific stock symbol targeted to process.
            config (BaseMetadata): The instance of data classes located in `schema.producer_schema`. 

        Returns:
            date: The precise calendar date from which the new data fetch should begin.
            
        Raises:
            ValueError: If the Postgres watermark table name is missing in the config.
        """
        if not config.table_watermark_name_postgres:
            raise ValueError(f"Missing Postgres Watermark table name in config for data type '{config.data_type}'.")
        with self.pg_conn.cursor() as cursor:
            query = sql.SQL("""
                SELECT updated_at 
                FROM ingestion.{table} 
                WHERE ticker = %(ticker)s AND data_type = %(data_type)s
            """).format(table=sql.Identifier(config.table_watermark_name_postgres))
            
            cursor.execute(query, {"ticker": ticker, "data_type": config.data_type})
            
            last_run_date = cursor.fetchone()['updated_at'].date() # type: ignore
            
            logger.debug(
                f"Smart start date retrieved. "
                f"Ticker: '{ticker}', Data Type: '{config.data_type}', Start Date: '{last_run_date}'."
            )
            return last_run_date
    
    
    
    def reconcile_watermark_from_lakehouse(self, config: BaseMetadata) -> None:
        """
        Rebuilds the Postgres watermark state by scanning the actual physical data stored in the Lakehouse.

        This acts as the pipeline's self-healing mechanism. If the Postgres tracking database 
        gets out of sync, or if tables are manually dropped, this method looks at the absolute 
        "Source of Truth" (the Iceberg tables) to figure out the exact current state.

        How it works:
        1. Table Check: Verifies if the Iceberg table for that data type exists. if not, it resets the Postgres state by calling `_reset_postgres_state`.
        2. Fast Scan: Finds the latest ingestion timestamp for each ticker over the last 3 years.
        3. Doomsday Reset: If no data exists in Iceberg, it wipes the state in both Postgres and Lakehouse to start hard reset.
        4. State Sync: If data exists, it updates Postgres with the most recent timestamps.

        Args:
            config (BaseMetadata): The instance of data classes located in `schema.producer_schema`.
        Raises:
            ValueError: If the Postgres watermark table name is missing.
        """
        if not config.table_watermark_name_postgres:
            raise ValueError(f"Missing Postgres Watermark table name in config for data type '{config.data_type}'.")
        
        logger.info(f"Starting Lakehouse sync. Scanning actual data to rebuild state for '{config.data_type}'.")
        
        # STEP 1: Verify if the target Bronze table even exists in the Lakehouse
        check_table_query = f"""
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = '{config.bronze_layer_name}'
        """
        with self.trino_conn.cursor() as cursor:
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone() 
            
       # If the table was manually dropped, trigger a hard reset for Postgres and exit early
        if not table_exists:
            logger.warning(f"⚠️ Bảng bronze.{config.bronze_layer_name} CHƯA TỒN TẠI, bắt đầu hard reset hệ thống.")
            self._reset_postgres_state(config)
            return
        
        current_year = datetime.now(ZoneInfo("UTC")).year
        lookback_year = current_year - 3
        
        # STEP 2: Fast scan the top of the Iceberg to find the latest ingestion dates
        # We limit the lookback to 3 years to optimize query performance on Trino
        query = f"""
            SELECT ticker, MAX(bronze_ingested_time) as last_ingested_time
            FROM bronze.{config.bronze_layer_name}
            WHERE year >= {lookback_year} AND data_type = '{config.data_type}'
            GROUP BY ticker
        """
        
        with self.trino_conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
        # STEP 3: Handle the results (Either Doomsday Reset or Sync State)
        if not results:
            # Scenario A: No data found in the Lakehouse. Trigger "Doomsday Protocol".
            # This happens when we want to completely wipe a data type and start from scratch.
            logger.warning(f"No Lakehouse data found. Initiating full state reset. Data Type: '{config.data_type}'.")
            
            # Wipe Postgres watermark state
            with self.pg_conn.transaction():
                with self.pg_conn.cursor() as pg_cur:
                    delete_pg_query = sql.SQL(
                        "DELETE FROM ingestion.{table} WHERE data_type = %(data_type)s"
                    ).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    pg_cur.execute(delete_pg_query, {"data_type": config.data_type})
                    
            logger.info(f"Postgres state cleared. Data Type: '{config.data_type}'.")
            
            # Wipe any lingering empty partitions in Iceberg to ensure a clean slate
            with self.trino_conn.cursor() as trino_cur:
                delete_iceberg_query = f"""
                    DELETE FROM bronze.{config.bronze_layer_name} 
                    WHERE data_type = '{config.data_type}'
                """
                trino_cur.execute(delete_iceberg_query)
            logger.info(f"✅ Đã dọn sạch dữ liệu cũ của '{config.data_type}' trong Iceberg.")
            
            return
        else:
            # Scenario B: Data found. Sync the latest timestamps back to Postgres.
            logger.info(f"Lakehouse data found. Syncing latest timestamps to Postgres. Data Type: '{config.data_type}'.")
            
            with self.pg_conn.transaction():
                with self.pg_conn.cursor() as pg_cur:
                    update_query = sql.SQL("""
                        UPDATE ingestion.{table}
                        SET updated_at = %(last_ingested_time)s
                        WHERE ticker = %(ticker)s AND data_type = %(data_type)s
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    # Prepare the payload for bulk update
                    payload = [
                            {
                                "ticker": row[0],
                                "last_ingested_time": row[1],
                                "data_type": config.data_type
                            }
                            for row in results
                        ]
                    
                    # executemany automatically loops through the payload safely
                    pg_cur.executemany(update_query, payload)
            logger.info(f"Watermark state successfully synchronized. Data Type: '{config.data_type}', Updated Count: {len(payload)}.")
     
     
            
    def _reset_postgres_state(self, config: BaseMetadata) -> None:
        """
        Internal helper: Performs a deep clean of all Postgres ingestion states for a specific data type.
        
        This is typically called during a "hard reset" (e.g., when the Lakehouse table is missing). 
        It wipes both the watermark tracking and the Kafka claim-check metadata, ensuring 
        the pipeline starts with a completely clean slate on its next run.

        Args:
            config (BaseMetadata): The instance of data classes located in `schema.producer_schema`.
        """
        logger.info(f"Initiating deep cleanup of Postgres states. Data Type: '{config.data_type}'.")
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as pg_cur:
                
                # STEP 1: Clear the main watermark tracking table
                if config.table_watermark_name_postgres:
                    cleanup_watermark_sql = sql.SQL("""
                        DELETE FROM ingestion.{table}
                        WHERE data_type = %(data_type)s;
                    """).format(table=sql.Identifier(config.table_watermark_name_postgres))
                    pg_cur.execute(cleanup_watermark_sql, {"data_type": config.data_type})
                    logger.info(f"Cleared watermark states. Table: '{config.table_watermark_name_postgres}'.")
                    
                # STEP 2: Clear the Kafka claim-check metadata table (if configured)
                if config.table_name_postgres:
                    cleanup_kafka_sql = sql.SQL("""
                        DELETE FROM ingestion.{table}
                        WHERE data_type = %(data_type)s;
                    """).format(table=sql.Identifier(config.table_name_postgres))
                    pg_cur.execute(cleanup_kafka_sql, {"data_type": config.data_type})
                    logger.info(f"Cleared Kafka claim-check metadata. Table: '{config.table_name_postgres}'.")
                    
        logger.info(f"Postgres state completely reset. Data Type: '{config.data_type}'. Ready for fresh ingestion.")



    def __enter__(self):
        """
        Enters the runtime context for the MetadataManager.
        
        Returns:
            MetadataManager: The current instance, ready for pipeline operations.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exits the runtime context and ensures all database connections are safely closed.
        
        This method is automatically triggered at the end of a 'with' block. It handles 
        the graceful teardown of both Postgres and Lakehouse (Trino) connections, 
        preventing connection leaks even if the pipeline crashes midway.
        """
        # Safely delegate the exit process to the underlying Postgres connection
        if hasattr(self, 'pg_conn') and self.pg_conn:
            self.pg_conn.__exit__(exc_type, exc_val, exc_tb) 
            
        # Safely delegate the exit process to the underlying Trino connection
        if hasattr(self, 'trino_conn') and self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
            
        # Log the teardown status based on whether an exception occurred
        if exc_type:
            logger.error(f"MetadataManager context closed due to an exception: {exc_val}", exc_info=True)
        else:
            logger.info("MetadataManager context closed gracefully. All database connections terminated.")

            