from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.constants import SourceRenderingBehavior
from datetime import datetime
from orchestration.dags.common.cosmos_config import profile_config, project_config, execution_config
from schema.producer_schema import Fundamental_1, Fundamental_2, Dividend, HistoricalQuotes, VNINDEXHistoricalQuotes
from ingestion.ingest_main import ingest_main
from load.load_main import load_main

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
    
    @task(task_display_name="ingest dividend events")
    def ingest_dividend_events():
        """
        **Phase: Ingest** - Dividend Events
        Generates Kafka payloads containing metadata to fetch recent dividend events from the source API.
        """
        ingest_main(model_cls=Dividend)
    task_1 = ingest_dividend_events()
    
    @task(task_display_name="load dividend events")
    def load_dividend_events():
        """
        **Phase: Load** - Dividend Events
        Consumes Kafka payloads, fetches actual dividend data, and loads it into the Lakehouse.
        """
        load_main(model_cls=Dividend)
    task_2 = load_dividend_events()
    
    dbt_transform_dividend = DbtTaskGroup(
        group_id="dbt_dividend_transformation",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
                             select=["+dividend_snapshot"],
                             source_pruning=True,
                             source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS, 
                             load_method=LoadMode.DBT_LS),
        operator_args={"fail_fast": True},
    )
    
    @task(task_display_name="ingest historical quotes")
    def ingest_historical_quotes():
        """
        **Phase: Ingest** - Historical Quotes
        Generates Kafka payloads to fetch stock prices. Uses the dividend snapshot to trigger full historical pulls for stocks requiring price adjustments.
        """
        ingest_main(model_cls=HistoricalQuotes)
    task_3 = ingest_historical_quotes()
    
    @task(task_display_name="load historical quotes")
    def load_historical_quotes():
        """
        **Phase: Load** - Historical Quotes
        Consumes payloads to fetch and load stock price data into the Lakehouse.
        """
        load_main(model_cls=HistoricalQuotes)
    task_4 = load_historical_quotes()
    
    @task(task_display_name="ingest VNINDEX historical quotes")
    def ingest_vnindex_historical_quotes():
        """
        **Phase: Ingest** - VNINDEX Quotes
        Generates Kafka payloads to fetch the general market index (VNINDEX) prices.
        """
        ingest_main(model_cls=VNINDEXHistoricalQuotes)
    task_5 = ingest_vnindex_historical_quotes() 
    
    @task(task_display_name="load VNINDEX historical quotes")
    def load_vnindex_historical_quotes():
        """
        **Phase: Load** - VNINDEX Quotes
        Consumes payloads to fetch and load VNINDEX price data into the Lakehouse.
        """
        load_main(model_cls=VNINDEXHistoricalQuotes)
    task_6 = load_vnindex_historical_quotes()
    
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
        operator_args={"fail_fast": True},
    )
    
    # Nối dây luồng chính
    task_1 >> task_2 >> dbt_transform_dividend >> task_3 >> task_4 >> task_5 >> task_6 >> dbt_transform_qmj_model  # type: ignore
    
my_qmj_dag = run_weekly()