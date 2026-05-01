from airflow.sdk import dag, task, task_group
from datetime import datetime
import os
from airflow.providers.docker.operators.docker import DockerOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import get_current_context
from common.infrastructure.minio_maintenance import LakehouseMaintenance
from common.clients.lakehouse_client import LakeHouseClient
from common.clients.postgres_client import PostgresClient
from airflow.providers.standard.operators.bash import BashOperator


DOCKER_NETWORK = "my_de_project_monitoring_net"
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

@dag(
    dag_id="system_maintenance_dag",
    dag_display_name="Maintenance: GC Nessie & Postgres Vacuum",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance", "monthly"],
)
def system_maintenance():
    """
    Orchestrates the monthly maintenance and garbage collection (GC) for the Data Lakehouse infrastructure.
    
    This DAG executes in 7 sequential steps divided into two main phases:

    Phase 1: Infrastructure Maintenance (Docker & SQL)
    - Step 1 (setup_gc_schema): Drops and recreates the PostgreSQL schema required for Nessie's native GC tracking.
    - Step 2 (clean_minio_files): Runs Nessie GC to find and delete unreferenced data files in MinIO that are older than 7 days.
    - Step 3 (clean_nessie_metadata): Purges old, unreferenced commits and metadata history directly from the Nessie catalog.
    - Step 4 (delete_catalog_tasks): Cleans up stale background catalog tasks on the main Nessie branch.
    - Step 5 (vacuum_postgres): Executes 'VACUUM ANALYZE' on PostgreSQL to reclaim physical disk space and update query statistics.

    Phase 2: Custom Lakehouse Maintenance (Python logic)
    - Step 6 (minio_cleanup): Accessing the physical inventory of MinIO,fetching a full list of files and compares it against the logical Nessie catalog using Postgres, and physically deletes any deep "orphan" files that Nessie missed.
    - Step 7 (iceberg_optimize): Manages an optimization queue. It fetches the latest Iceberg tables from Trino system tables and executes table compaction to merge small files and improve query speed.
    """
    
    @task_group(group_id='infrastructure_maintenance',
            group_display_name="Infrastructure Maintenance")
    def infrastructure_maintenance_group():
        jdbc_args = [
                "--jdbc-url", "jdbc:postgresql://postgres:5432/platform_db?currentSchema=nessie_gc",
                "--jdbc-user", POSTGRES_USER,
                "--jdbc-password", POSTGRES_PASSWORD
            ]

        # Step 1: Initialize or reset the SQL schema for Nessie Garbage Collection in database
        setup_gc_schema = DockerOperator(
            task_id='setup_gc_schema',
            image='ghcr.io/projectnessie/nessie-gc:0.107.4', 
            command=["create-sql-schema"] + jdbc_args + ["--jdbc-schema", "DROP_AND_CREATE"],
            network_mode=DOCKER_NETWORK,
            docker_url='unix://var/run/docker.sock',
            environment={"AWS_REGION": "us-east-1"},
            auto_remove='force', 
            mount_tmp_dir=False
        )

        # Step 2: Remove orphan files from MinIO storage older than 7 days
        # Uses Jinja macro to calculate the safe deletion timestamp
        safe_time = "{{ (data_interval_end - macros.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ') }}"
        
        clean_minio_files = DockerOperator(
            task_id='clean_minio_files',
            image='ghcr.io/projectnessie/nessie-gc:0.107.4',
            command=["gc", "-c", "P7D", "--max-file-modification", safe_time, 
                        "--uri", "http://nessie:19120/api/v2"] + jdbc_args + [
                        "-I", f"s3.access-key-id={MINIO_ROOT_USER}",
                        "-I", f"s3.secret-access-key={MINIO_ROOT_PASSWORD}",
                        "-I", "s3.endpoint=http://minio:9000",
                        "-I", "s3.path-style-access=true"],
            network_mode=DOCKER_NETWORK,
            docker_url='unix://var/run/docker.sock',
            environment={"AWS_REGION": "us-east-1"},
            auto_remove='force',
            mount_tmp_dir=False
        )

        nessie_env = {
            "nessie.version.store.type": "JDBC",
            "nessie.version.store.persist.jdbc.datasource": "postgresql",
            "nessie.version.store.persist.jdbc.schema": "nessie",
            "quarkus.datasource.postgresql.jdbc.url": "jdbc:postgresql://postgres:5432/platform_db?currentSchema=nessie",
            "quarkus.datasource.postgresql.username": POSTGRES_USER, 
            "quarkus.datasource.postgresql.password": POSTGRES_PASSWORD,
            "nessie.catalog.service.s3.default-options.region": "us-east-1"
        }
        
        # Step 3: Purge unreferenced metadata and commits from the Nessie repository
        clean_nessie_metadata = DockerOperator(
            task_id='clean_nessie_metadata',
            image='ghcr.io/projectnessie/nessie-server-admin:0.107.4', 
            command=[
                "cleanup-repository",
                "--referenced-grace=P1D",
                "--commit-rate=50",
                "--obj-rate=1000",
                "--scan-obj-rate=2000",
                "--purge-obj-rate=500"
            ],
            network_mode=DOCKER_NETWORK,
            docker_url='unix://var/run/docker.sock',
            auto_remove='force',
            mount_tmp_dir=False,
            environment=nessie_env
        )

        # Step 4: Clear out stale catalog tasks on the main branch
        delete_catalog_tasks = DockerOperator(
            task_id='delete_catalog_tasks',
            image='ghcr.io/projectnessie/nessie-server-admin:0.107.4',
            command=[
                "delete-catalog-tasks",
                "--ref", "main",
                "--batch", "500"
            ],
            network_mode=DOCKER_NETWORK,
            docker_url='unix://var/run/docker.sock',
            auto_remove='force',
            mount_tmp_dir=False,
            environment=nessie_env
        )

        # Step 5: Perform Postgres VACUUM ANALYZE to reclaim disk space 
        vacuum_postgres = SQLExecuteQueryOperator(
            task_id="vacuum_postgres",
            conn_id="PLATFORM_DB",
            sql="VACUUM ANALYZE;",
            autocommit=True,
        )
        setup_gc_schema >> clean_minio_files >> clean_nessie_metadata >> delete_catalog_tasks >> vacuum_postgres # type: ignore
    
    @task(task_display_name= "Minio orphan files cleanup")
    def minio_cleanup():
        with LakehouseMaintenance(pg_client=PostgresClient(), lake_client=LakeHouseClient()) as lm:
            lm.minio_maintenance()
    
    @task(task_display_name= "Iceberg Optimize Files ")
    def iceberg_optimize():
        with LakehouseMaintenance(pg_client=PostgresClient(), lake_client=LakeHouseClient()) as lm:
            lm.run_full_iceberg_maintenance()
    
    infra_maintenance = infrastructure_maintenance_group()
    infra_maintenance >> minio_cleanup() >> iceberg_optimize() # type: ignore
    
maintenance_dag = system_maintenance()

@dag(
    dag_id="system_cleanup_airflow_logs",
    dag_display_name="Maintenance: Airflow Log Cleanup",
    schedule="0 0 * * *",  # Runs daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance", "daily"],
    default_args={"retries": 1}
)
def airflow_log_cleanup():
     """
    Maintenance Pipeline: Airflow Log Cleanup
    
    This DAG performs routine garbage collection on Airflow's internal log directory to free up disk space and prevent storage overload.
    
    Workflow Steps:
    1. Delete Old Files: Finds and removes all log files that are older than the specified threshold (14 days).
    2. Remove Empty Directories: Scans the log directory and deletes any empty folders left behind after the file deletion.
    """
    AIRFLOW_LOG_DIR = "/opt/airflow/logs"
    DAYS_TO_KEEP = 14  # Configurable threshold for log retention
    
    @task(task_display_name="purge old logs airflow")
    def purge_old_logs():
        purge_old_logs = BashOperator(
            task_id="purge_old_logs",
            bash_command=f"""
                echo "Starting Airflow log cleanup. Target: older than {DAYS_TO_KEEP} days."
                
                # Step 1: Find and delete all files (-type f) older than {DAYS_TO_KEEP} days (-mtime +{DAYS_TO_KEEP})
                find {AIRFLOW_LOG_DIR} -type f -mtime +{DAYS_TO_KEEP} -delete
                echo "Old log files deleted."
                
                # Step 2: Find and delete all empty directories (-type d -empty)
                find {AIRFLOW_LOG_DIR} -type d -empty -delete
                echo "Empty directories deleted."
                
                echo "Cleanup completed successfully!"
            """
        )