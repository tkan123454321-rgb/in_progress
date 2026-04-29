from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.constants import SourceRenderingBehavior
from datetime import datetime
from orchestration.dags.common.cosmos_config import profile_config, project_config, execution_config
from schema.producer_schema import Fundamental_1, Fundamental_2
from ingestion.ingest_main import ingest_main
from load.load_main import load_main
from orchestration.dags.task_groups import dividend_processing_group, historical_quotes_task_group, quarter_financial_reports_group
from orchestration.dags.export_tasks import csv_frontend_serving_task
#-------------------------------------------------------------------------------------------------------------
@dag(
    dag_id="gold_dim_company_dag",
    dag_display_name="Pipeline: Gold Dim Company & Fundamentals",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weekly"],
    default_args={"retries": 1},
    is_paused_upon_creation=False
)
def create_gold_dim_company():
    """
    Pipeline: Master Dimension & Fundamentals
    
    This DAG builds the company dimension table and processes fundamental data from Seed to Gold layer.
    
    Workflow Steps:
    1. Base List (Seed): Loads the static company list into `bronze_dim_company`.
    2. API Data (Ingest & Load): Fetches fresh `fundamental_1` (share structure, valuation) and `fundamental_2` (listing status) data into the Lakehouse.
    3. Silver Layer: Cleans and deduplicates the raw data (`silver_dim_company`, `silver_fundamental_1`, `silver_fundamental_2`).
    4. Gold Layer: Joins silver fundamentals with the company list to build the final `gold_dim_company` table.
    5. Snapshot (SCD Type 2): Runs `dim_company_snapshot` to track historical data changes over time.
    """
    @task(task_display_name="ingest fundamental data 1")
    def ingest_fundamental_data_1():
        """
        **Phase: Ingest (Metadata to Kafka)** - Fundamental 1
        
        Acts as the payload generator. It checks PostgreSQL tracking tables and watermarks to identify tickers that need processing today. It then generates JSON payloads containing fetch metadata (URLs, batch ID, timestamps) and publishes them to a Kafka topic.
        """
        ingest_main(model_cls=Fundamental_1)
    task_1 = ingest_fundamental_data_1()
    
    @task(task_display_name="ingest fundamental data 2")
    def ingest_fundamental_data_2():
        """
        **Phase: Ingest (Metadata to Kafka)** - Fundamental 2
        
        Acts as the payload generator. It checks PostgreSQL tracking tables and watermarks to identify tickers that need processing today. It then generates JSON payloads containing fetch metadata (URLs, batch ID, timestamps) and publishes them to a Kafka topic.
        """
        ingest_main(model_cls=Fundamental_2)
    task_2 = ingest_fundamental_data_2()
    
    @task(task_display_name="load fundamental data 1")
    def load_fundamental_data_1():
        """
        **Phase: Load (API to Lakehouse)** - Fundamental 1
        
        Consumes metadata payloads from Kafka, triggering external API calls to fetch the actual financial data (share structure, valuation, etc.). The raw data is buffered, cleansed via Polars, and batch-loaded into the Lakehouse. Includes an automated 2-pass strategy (Primary & Retry) to handle transient API failures.
        """
        load_main(model_cls=Fundamental_1)
    task_3 = load_fundamental_data_1()

    @task(task_display_name="load fundamental data 2")
    def load_fundamental_data_2():
        """
        **Phase: Load (API to Lakehouse)** - Fundamental 2
        
        Consumes metadata payloads from Kafka, triggering external API calls to fetch the actual listing status and exchange data. The raw data is buffered, cleansed via Polars, and batch-loaded into the Lakehouse. Includes an automated 2-pass strategy (Primary & Retry) to handle transient API failures.
        """
        load_main(model_cls=Fundamental_2)
    task_4 = load_fundamental_data_2()
    
    dbt_transform = DbtTaskGroup(
        group_id="dbt_gold_dim_company_transformation",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
                             select=["+dim_company_snapshot"],
                             source_pruning=True,
                             source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS, 
                             load_method=LoadMode.DBT_LS),
        operator_args={"fail_fast": True},
    )
    
    
    task_1 >> task_2 >> task_3 >> task_4 >> dbt_transform  # type: ignore

my_dag = create_gold_dim_company()


#-------------------------------------------------------------------------------------------------------------

@dag(
    dag_id="historical_quotes_dag",
    dag_display_name="Pipeline: Historical Quotes Weekly Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weekly"],
    default_args={"retries": 1},
    is_paused_upon_creation=False
)
def run_weekly():
    """
    Pipeline: Historical Quotes & QMJ Model
    
    This DAG is triggered weekly to process dividend events, fetch historical stock/VNINDEX prices, and calculate the final business metrics scores and update the One Big Table.
    
    Workflow Steps:
    1. Dividend Processing: Ingests and loads dividend events, running them through dbt to create the `dividend_snapshot`. This acts as a signal to identify which stocks issued dividends, triggering price adjustments and full historical data pulls for those specific tickers.
    2. Price Ingestion: After the dividend branch finishes, it fetches fresh historical quotes for the active stock list and the VNINDEX.
    3. Final Transformation (QMJ): Runs the dbt models (starting from staging) to calculate metrics and update the final One-Big-Table (`obt_web`), which directly serves the end-users on the Streamlit app.
    """
    
    dividend_group = dividend_processing_group()
    
    historical_quotes_group = historical_quotes_task_group()
    
    dbt_transform_qmj_model = DbtTaskGroup(
        group_id="dbt_qmj_model_transformation",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
                             select=["staging_historical_quotes+"],
                             source_pruning=True,
                             source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS, 
                             load_method=LoadMode.DBT_LS),
        operator_args={"fail_fast": True}
    )
    
    frontend_serving_task = csv_frontend_serving_task()
    
    dividend_group >> historical_quotes_group >> dbt_transform_qmj_model >> frontend_serving_task  # type: ignore
    
my_qmj_dag = run_weekly()

#-------------------------------------------------------------------------------------------------------------

@dag(
    dag_id="financial_quarter_reports_dag",
    dag_display_name="Pipeline: Financial Quarter Dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quarterly"],
    default_args={"retries": 0},
    is_paused_upon_creation=False
)
def quarterly_dag(): 
    """
    Pipeline: Quarterly Financial Reports
    
    This DAG is triggered manually on-demand (typically every quarter) to fetch and process quarterly financial statements and fundamental data.
    
    Workflow Steps:
    1. Ingestion & Load: Fetches fresh quarterly data including the Income Statement, Balance Sheet, Indirect Cash Flow, and Quarterly Fundamentals, then loads them into the Lakehouse.
    2. Transformation (dbt): Runs the dbt models for the quarterly financial data to clean, transform, and integrate the raw reports.
    3. Frontend Serving: Exports the updated One-Big-Table to a local CSV file, which serves as the data source for the Streamlit web application.
    """
    
    financial_quarter_group = quarter_financial_reports_group()
    
    dbt_transform_financial_quarter = DbtTaskGroup(
        group_id="dbt_financial_quarter_transformation",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
                             select=["tag:quarter+"],
                             source_pruning=True,
                             source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS, 
                             load_method=LoadMode.DBT_LS),
        operator_args={"fail_fast": True}
    )
    
    frontend_serving_task = csv_frontend_serving_task()
    
    financial_quarter_group >> dbt_transform_financial_quarter >> frontend_serving_task  # type: ignore
    
    
    
my_financial_quarter_dag = quarterly_dag()
    
