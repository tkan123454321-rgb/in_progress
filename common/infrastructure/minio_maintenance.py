from common.clients.lakehouse_client import LakeHouseClient
from common.clients.postgres_client import PostgresClient
from common.core.logger_config import setup_logger
from datetime import datetime, timedelta, timezone
from psycopg import sql
from typing import Sequence, Generator
from trino.exceptions import Error as TrinoError

logger = setup_logger(component="maintenance")


class LakehouseMaintenance:
    """
    Orchestrates maintenance operations for the Lakehouse infrastructure.
    
    This class is responsible for two primary background processes:
    1. MinIO Garbage Collection (GC): Identifies and deletes orphan physical files 
       stored in MinIO that are no longer referenced by the Nessie catalog.
    2. Iceberg Table Optimization: Compacts small files (target size 128MB) and 
       optimizes manifest files via Trino to ensure high-performance querying.
    Usage Example:
        with LakehouseMaintenance(PostgresClient(), LakeHouseClient()) as maintainer:
            # Perform maintenance tasks
            maintainer.minio_maintenance()
            maintainer.run_full_iceberg_maintenance()
    """
    def __init__(self, pg_client: PostgresClient, lake_client: LakeHouseClient):
        """
        Initializes the maintenance orchestrator using existing client connections.

        Args:
            pg_client: Client for PostgreSQL metadata database operations.
            lake_client: Client containing Trino, S3 (MinIO), and Catalog connections.
        """
        self.pg_conn = pg_client.get_db_connection(db_name="platform_db")
        self.trino_conn = lake_client._get_trino_connection()
        self.s3_client = lake_client.s3_client
        self.catalog = lake_client.catalog
        self.REQUIRED_SCHEMAS = lake_client.REQUIRED_SCHEMAS
        self.BUCKET_NAME = lake_client.BUCKET_NAME
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Safely delegates the teardown of database connections.
        """
        if self.pg_conn:
            self.pg_conn.__exit__(exc_type, exc_val, exc_tb)
        if self.trino_conn:
            self.trino_conn.__exit__(exc_type, exc_val, exc_tb)
        if exc_type:
            logger.error(f"Maintenance context closed abnormally. Exception: {exc_val}", exc_info=True)
        else:
            logger.info("Maintenance context closed successfully. All connections terminated.")
    
    # ==========================================
    # PHASE 1: MINIO GARBAGE COLLECTION
    # ==========================================
    
    
    def _prepare_temp_minio_table(self) -> None:
        """
        Drops and recreates a temporary PostgreSQL table to store the current physical 
        object locations scanned from MinIO.
        
        """
        setup_sql = """
            DROP TABLE IF EXISTS nessie_gc.temp_minio_object_locations;
            CREATE TABLE nessie_gc.temp_minio_object_locations (
                location VARCHAR PRIMARY KEY,
                scanned_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """
        self.pg_conn.execute(setup_sql)
        logger.debug("Prepared temporary table: 'nessie_gc.temp_minio_object_locations'.")

    def _insert_minio_location_batch(self, batch: Sequence[tuple]) -> None:
        """
        Inserts a batch of physical MinIO paths into the temporary tracking table.
        """
        insert_sql = "INSERT INTO nessie_gc.temp_minio_object_locations (location) VALUES (%s)"
        with self.pg_conn.cursor() as cur:
            cur.executemany(insert_sql, batch)

    def _swap_minio_location_tables(self) -> None:
        """
        Promotes the temporary MinIO location table to the primary tracking table.
        """
        swap_sql = """
            DROP TABLE IF EXISTS nessie_gc.minio_object_locations CASCADE;
            ALTER TABLE nessie_gc.temp_minio_object_locations RENAME TO minio_object_locations;
        """
        with self.pg_conn.transaction():
            with self.pg_conn.cursor() as cur:
                cur.execute(swap_sql)
        logger.debug("Swapped temporary MinIO location table to production table.")
    
    
    def _yield_orphan_location_batches(self) -> Generator[str, None, None]:
        """
        Executes a set-difference (EXCEPT) query to find orphan files.
        Compares physical paths in MinIO against active logical paths in Nessie.
        
        Yields:
            Generator[str, None, None]: Orphan S3 object keys in batches.
        """
        set_ram_sql = "SELECT set_config('work_mem', '50MB', true);"
        query = sql.SQL( """
            SELECT location 
            FROM nessie_gc.minio_object_locations
            EXCEPT
            SELECT REPLACE(base_location, 's3://financial-data-lake/', '')
            FROM nessie_gc.gc_live_set_content_locations;
        """)
        
        with self.pg_conn.transaction():
            self.pg_conn.execute(set_ram_sql)
            with self.pg_conn.cursor(name="orphan_stream_cursor") as cur:
                cur.execute(query)
                
                while True:
                    batch = cur.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        yield row['location'] # type: ignore
        
    
    
    def _yield_schema_locations(self) -> Generator[Sequence[tuple], None, None]:
        """
        Scans the MinIO bucket using the S3 paginator to fetch all existing objects.
        
        Yields:
            Generator[Sequence[tuple], None, None]: Batches of object prefixes formatted for SQL inserts.
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for schema_name in self.REQUIRED_SCHEMAS:
            prefix_path = f"{schema_name}/"
            page_paginator = paginator.paginate(Bucket=self.BUCKET_NAME, Delimiter='/', Prefix=prefix_path)
            for page in page_paginator:
                if 'CommonPrefixes' in page:
                    batch=[]
                    for obj in page['CommonPrefixes']:
                        folder_path = obj['Prefix']
                        batch.append((folder_path,))
                    yield batch

    def _clean_orphan_location(self, location: str) -> None:
        """
        Physically deletes an orphan directory/prefix from the MinIO bucket.
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.BUCKET_NAME, Prefix=location)
        for page in pages:
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                for obj in page['Contents']:
                    response = self.s3_client.delete_objects(Bucket=self.BUCKET_NAME, 
                                                            Delete={
                                                                'Objects': objects_to_delete,
                                                                'Quiet': True
                                                            })
                    if 'Errors' in response:
                        logger.error(f"S3 Deletion Error. Prefix: '{location}', Details: {response['Errors']}")


    def minio_maintenance(self) -> None:
        """
        Orchestrates the full Garbage Collection cycle for MinIO.
        """
        logger.info("Starting MinIO Garbage Collection process.")
        try:
            # STEP 1: Build the Physical Inventory
            # We scan the MinIO storage to fetch all existing physical folder paths 
            # and save them into a Postgres table. We wrap this in a transaction 
            # to ensure the table swapping is safe and atomic.
            with self.pg_conn.transaction():
                self._prepare_temp_minio_table()
                for batch in self._yield_schema_locations():
                    self._insert_minio_location_batch(batch)
                self._swap_minio_location_tables()
            logger.info("Successfully synced MinIO physical object locations to PostgreSQL.")
            
            # STEP 2: Identify and Purge Orphans
            # We compare the physical MinIO inventory against the logical Lakehouse catalog (Nessie).
            # Any path existing in MinIO but missing from Nessie is an "orphan". 
            # We iterate through these orphans and physically delete them from S3 to reclaim storage.
            total_deleted = 0
            for orphan_loc in self._yield_orphan_location_batches():
                self._clean_orphan_location(orphan_loc)
                total_deleted += 1
                
            # STEP 3: Summary and Reporting
            if total_deleted == 0:
                logger.info("MinIO GC completed. Storage is clean, no orphan objects found.")
            else:
                logger.info(f"MinIO GC completed successfully. Purged Orphan Count: {total_deleted}.")      
        except Exception as e:
            logger.error(f"MinIO Garbage Collection failed. Transaction rolled back. Error: {e}", exc_info=True)
    
    # ==========================================
    # PHASE 2: ICEBERG TABLE OPTIMIZATION
    # ==========================================
    def _prepare_maintenance_queue(self):
        """
        Validates the Iceberg optimization queue table. If the queue is empty or 
        older than 7 days, it clears the state to trigger a fresh metadata fetch.

        Returns:
            bool: True if the queue needs to be refreshed from Trino, False otherwise.
        """
        
        query_create_table = sql.SQL("""
                        CREATE TABLE IF NOT EXISTS iceberg_maintenance.iceberg_optimize (
                            schema_name VARCHAR(100),
                            table_name VARCHAR(255),
                            status VARCHAR(20) DEFAULT 'PENDING',
                            batch_created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            last_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (schema_name, table_name)
            )
            """)
        
        self.pg_conn.execute(query_create_table)
        cursor_check = self.pg_conn.execute("SELECT COUNT(*) as total_count, MIN(batch_created_at) as oldest_date FROM iceberg_maintenance.iceberg_optimize;")
        row = cursor_check.fetchone()
        print(row)
        if row['total_count'] == 0: # type: ignore
            logger.info("Optimization queue is empty. Refreshing metadata required.")
            return True 
            
        if row['oldest_date'] is not None: # type: ignore
            # Check if the oldest record is beyond the 7-day threshold
            if row['oldest_date'] < datetime.now(timezone.utc) - timedelta(days=7):  # type: ignore
                logger.info("Optimization queue expired (older than 7 days). Truncating queue.")
                self.pg_conn.execute("TRUNCATE TABLE iceberg_maintenance.iceberg_optimize;")
                return True
                
        return False
    
    
    
    def _refresh_queue_from_trino(self) -> None:
        """
        Fetches the complete list of Iceberg tables from Trino and populates the Postgres queue.
        """
        with self.trino_conn.cursor() as cur:
            cur.execute("SELECT table_schema, table_name FROM system.iceberg_tables")
            rows = cur.fetchall()
        logger.info(f"Retrieved {len(rows)} tables from Trino catalog. Populating optimization queue.")

        with self.pg_conn.cursor() as pg_cur:
            pg_cur.executemany("""
                INSERT INTO iceberg_maintenance.iceberg_optimize (schema_name, table_name) 
                VALUES (%s, %s)
                ON CONFLICT (schema_name, table_name) DO NOTHING
            """, rows)
        logger.info("Successfully populated Postgres optimization queue with Lakehouse tables.")
    
    
    
    def _run_optimize(self) -> None:
        """
        Processes the optimization queue by executing Trino OPTIMIZE commands on PENDING tables.
        """
        logger.info("Initiating Iceberg table optimization process.")
        
        # STEP 1: Fetch Pending Tasks
        # Retrieve all tables that haven't been optimized yet in the current maintenance cycle.
        
        cursor = self.pg_conn.execute("""
            SELECT schema_name, table_name 
            FROM iceberg_maintenance.iceberg_optimize 
            WHERE status = 'PENDING'
        """)
        tasks = cursor.fetchall() 
        if not tasks:
            logger.info("Optimization queue is clear. All tables are in SUCCESS state.")
            return

        logger.info(f"Beginning compaction for {len(tasks)} pending tables.")

        with self.trino_conn.cursor() as trino_cur:
            logger.info(f"Starting optimization for {len(tasks)} tables...")
            for task in tasks:
                try:
                    # STEP 2: Execute Trino Compaction
                    # Command Trino to merge small data files into larger chunks (target 128MB) 
                    # to solve the "small file problem" and improve read performance.
                    schema = task['schema_name'] # type: ignore
                    table = task['table_name'] # type: ignore
                    table_full_name = f"{schema}.{table}" 
                    trino_cur.execute(f"ALTER TABLE {table_full_name} EXECUTE optimize(file_size_threshold => '128MB')")
                    trino_cur.fetchall() 
                    # Command Trino to clean up and rewrite the Iceberg metadata manifest files.
                    trino_cur.execute(f"ALTER TABLE {table_full_name} EXECUTE optimize_manifests")
                    trino_cur.fetchall()
                    # STEP 3: Update Task State
                    # Mark the task as 'SUCCESS' in Postgres so we don't process it again 
                    # if the pipeline restarts.
                    update_query = """
                        UPDATE iceberg_maintenance.iceberg_optimize 
                        SET status = 'SUCCESS', last_updated_at = CURRENT_TIMESTAMP 
                        WHERE schema_name = %s AND table_name = %s
                    """
                    self.pg_conn.execute(update_query, (schema, table))
                    
                    logger.info(f"Optimization completed successfully. Table: '{table_full_name}'.")
                    
                except Exception as e:
                    # If one table fails (e.g., due to a Trino timeout), we log the error 
                    # and continue to the next table instead of crashing the whole loop.
                    logger.error(f"Optimization failed. Table: '{table_full_name}', Error: {e}")
    
    def run_full_iceberg_maintenance(self) -> None:
        """
        The primary orchestrator method for Iceberg maintenance. 
        It manages the lifecycle of checking, refreshing, and executing the optimization queue.
        """
        logger.info("STARTING PIPELINE: Lakehouse Iceberg Maintenance.")
        try:
            # STEP 1: Queue Assessment
            # We check the Postgres tracking table to see if our optimization queue 
            # is empty or if the records are stale (e.g., older than our 7-day threshold).
            needs_refresh = self._prepare_maintenance_queue()
            
            # STEP 2: Catalog Refresh (If Required)
            # If the queue is outdated or empty, we fetch the latest list of Iceberg tables 
            # directly from the Trino system catalog and stage them as 'PENDING'.
            if needs_refresh:
                self._refresh_queue_from_trino()
                
            # STEP 3: Execute Optimization
            # We iterate over all 'PENDING' tables in our queue and command Trino 
            # to compact small files and optimize manifests.
            self._run_optimize()
            
            logger.info("Lakehouse Iceberg Maintenance pipeline completed successfully.")
            
        except Exception as e:
            # A final safety net to catch any catastrophic failures in the orchestration flow
            logger.critical(f"CRITICAL FAILURE in Iceberg Maintenance pipeline. Error: {e}", exc_info=True)
                
                
        

                

