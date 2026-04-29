from airflow.sdk import dag, task, task_group
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.constants import SourceRenderingBehavior
from datetime import datetime
from orchestration.dags.common.cosmos_config import profile_config, project_config, execution_config
from schema.producer_schema import Dividend, HistoricalQuotes, VNINDEXHistoricalQuotes, FinancialReportsQuarterBalanceSheet, FinancialReportsQuarterIncomeStatement, FinancialReportsQuarterCashFlowIndirect, FundamentalQuarter
from ingestion.ingest_main import ingest_main
from load.load_main import load_main


@task_group(
    group_id="dividend_processing_group", 
    group_display_name="Dividend Processing"
)
def dividend_processing_group():
    """
    Extracted into a TaskGroup to improve readability, enhance error traceability, keep the main DAG clean, and strictly adhere to the DRY (Don't Repeat Yourself) principle.
    """
    @task(task_display_name="ingest dividend events")
    def ingest_dividend_events():
        ingest_main(model_cls=Dividend)
    
    @task(task_display_name="load dividend events")
    def load_dividend_events():
        load_main(model_cls=Dividend)
    
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
    
    # Nối luồng nội bộ trong Group 1
    ingest_dividend_events() >> load_dividend_events() >> dbt_transform_dividend # type: ignore


@task_group(
    group_id="historical_quotes_processing", 
    group_display_name="Historical Quotes Processing"
)
def historical_quotes_task_group():
    """
    Extracted into a TaskGroup to improve readability, enhance error traceability, keep the main DAG clean, and strictly adhere to the DRY (Don't Repeat Yourself) principle.
    """
    @task(task_display_name="ingest historical quotes")
    def ingest_historical_quotes():
        ingest_main(model_cls=HistoricalQuotes)
    
    @task(task_display_name="load historical quotes")
    def load_historical_quotes():
        load_main(model_cls=HistoricalQuotes)
    
    @task(task_display_name="ingest VNINDEX historical quotes")
    def ingest_vnindex_historical_quotes():
        ingest_main(model_cls=VNINDEXHistoricalQuotes)
        
    @task(task_display_name="load VNINDEX historical quotes")
    def load_vnindex_historical_quotes():
        load_main(model_cls=VNINDEXHistoricalQuotes)
    
    ingest_historical_quotes() >> load_historical_quotes() >> ingest_vnindex_historical_quotes() >> load_vnindex_historical_quotes()# type: ignore



@task_group(
    group_id="quarter_financial_reports_processing", 
    group_display_name="Financial Quarter Reports Processing"
)
def quarter_financial_reports_group():
    """
    Extracted into a TaskGroup to improve readability, enhance error traceability, keep the main DAG clean, and strictly adhere to the DRY (Don't Repeat Yourself) principle.
    """
    @task(task_display_name="ingest quarter balance sheet")
    def ingest_quarter_balance_sheet():
        ingest_main(model_cls=FinancialReportsQuarterBalanceSheet)
    
    @task(task_display_name="load quarter balance sheet")
    def load_quarter_balance_sheet():
        load_main(model_cls=FinancialReportsQuarterBalanceSheet)
    
    @task(task_display_name="ingest quarter income statement")
    def ingest_quarter_income_statement():
        ingest_main(model_cls=FinancialReportsQuarterIncomeStatement)
    
    @task(task_display_name="load quarter income statement")
    def load_quarter_income_statement():
        load_main(model_cls=FinancialReportsQuarterIncomeStatement)
    
    @task(task_display_name="ingest quarter cash flow indirect")
    def ingest_quarter_cash_flow_indirect():
        ingest_main(model_cls=FinancialReportsQuarterCashFlowIndirect)
    
    @task(task_display_name="load quarter cash flow indirect")
    def load_quarter_cash_flow_indirect():
        load_main(model_cls=FinancialReportsQuarterCashFlowIndirect)
    
    @task(task_display_name="ingest quarter fundamental")
    def ingest_quarter_fundamental():
        ingest_main(model_cls=FundamentalQuarter)   
    
    @task(task_display_name="load quarter fundamental")
    def load_quarter_fundamental():
        load_main(model_cls=FundamentalQuarter)
    
    ingest_quarter_balance_sheet() >> load_quarter_balance_sheet() >> ingest_quarter_income_statement() >> load_quarter_income_statement() >> ingest_quarter_cash_flow_indirect() >> load_quarter_cash_flow_indirect() >> ingest_quarter_fundamental() >> load_quarter_fundamental() # type: ignore
        
        
    