from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import ExecutionMode, InvocationMode, SourceRenderingBehavior
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.airflow.dag import DbtDag
from airflow.sdk import dag, task
from datetime import datetime
from common.core.logger_config import setup_logger


logger = setup_logger("qmj_pipeline")

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





@dag(
    dag_id="qmj_raw_stock_list",
    dag_display_name="Pipeline: Stock List Ingestion (Raw to Silver)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["qmj", "main_pipeline"],
    default_args={"retries": 1},
    is_paused_upon_creation=False
)
def ingest_raw_stock_list_dag():
    """
    Company Master List Pipeline (dbt Seed to Silver)
    
    This DAG builds the reference data for QMJ project. 
    It creates stock tickers table and standardizes them into a master dimension table.
    
    **Workflow Steps:**
    * **Seed (Bronze):** Uses `dbt seed` to load a static, pre-categorized list of companies (including industry and company type) into the Bronze layer (`bronze_dim_company`).
    * **Transform & Load (Silver):** deduplicates the raw data, materializing it into the Silver layer (`silver_dim_company`).
    * **Downstream Usage:** This Silver table acts as the single source of truth (Master Dimension Table), providing reference data for downstream fundamental models (e.g., `fundamental_1` and `fundamental_2`).
    """
    dbt_transform = DbtTaskGroup(
        group_id="dbt_silver_raw_ticker_list_transformation",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
                             select=["+silver_dim_company"],
                             source_pruning=True,
                             source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS, 
                             load_method=LoadMode.DBT_LS),
        operator_args={"fail_fast": True},
    )
my_dag =ingest_raw_stock_list_dag()

@dag(
    dag_id="test_log",
    dag_display_name="Test Log DAG",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"],
    default_args={"retries": 1},
    is_paused_upon_creation=False
)
def test_log_dag():
    @task
    def log_test():
        logger.info("This is a test log message.")
    log_test()
    
test = test_log_dag()