from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import ExecutionMode, InvocationMode, SourceRenderingBehavior
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.airflow.dag import DbtDag
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from schema.producer_schema import OriginalTickerList
from load.lakehouse_loader import LakehouseLoader

DBT_PROJECT_PATH = "/opt/airflow/transform" 
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="transform", 
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml"
)
project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH,
                               install_dbt_deps=False)

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH,
                                   execution_mode=ExecutionMode.LOCAL,
                                   invocation_mode=InvocationMode.SUBPROCESS)

render_config = RenderConfig(select=["+silver_dim_company"],
                             source_pruning=True,
                             source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS, 
                             load_method=LoadMode.DBT_LS)

@dag(
    dag_id="qmj_raw_stock_list",
    dag_display_name="Pipeline: Stock List Ingestion (Raw to Silver)",
    description="xtract raw stock data from 3 exchanges, apply data cleansing rules, and load into the Silver layer.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["qmj", "main_pipeline"],
    default_args={"retries": 1},
    is_paused_upon_creation=False
)
def ingest_raw_stock_list_dag():
    """
    ### Stock List Ingestion Pipeline (ETL)
    
    This DAG is responsible for building the foundational reference data for the QMJ project. 
    It executes a complete ETL process to provide a master list of stocks for downstream dbt models.
    
    Workflow Steps:
    Extract: Retrieves a master list of 1500+ stock tickers across 3 major exchanges (HOSE, HNX, UPCOM) using the API from the vnstock library.
    Transform: Cleans and standardizes the raw data in-memory using polars.
    Load: Ingests the transformed data into the Silver layer of the Lakehouse (Trino/Iceberg).
    """
    
    @task(task_id="extract_transform_load_stock_list", retries=3,retry_delay=timedelta(minutes=1))
    def extract():
        loader = LakehouseLoader()
        loader._put_original_ticker_list(OriginalTickerList)
    
    extract_task = extract()
    
    dbt_transform = DbtTaskGroup(
        group_id="dbt_silver_raw_ticker_list_transformation",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"fail_fast": True},
    )
    extract_task >> dbt_transform # type: ignore
ingest_raw_stock_list_dag()