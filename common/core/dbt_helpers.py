from pathlib import Path
from common.clients.lakehouse_client import LakeHouseClient
from common.core.logger_config import setup_logger

logger = setup_logger(component="load")


def export_silver_company_to_seed():
    """
    Extracts data from the `silver.silver_dim_company` table and exports it to a CSV file for dbt seed.
    This serves as a reliable backup source in case the original table or API fails.
    """
    try:
        client = LakeHouseClient()
        table_path = "silver.silver_dim_company"
        logger.info(
            f"Initiating data extraction for dbt seed. Source Table: '{table_path}'."
        )
        # STEP 1: Connect to Lakehouse and fetch data
        table = client.catalog.load_table(table_path)
        # Utilize Polars for high-performance memory scanning
        df = table.scan().to_polars()
        # STEP 2: clean data by dropping internal audit columns
        # We strip these out because dbt will generate its own fresh audit trails
        # (e.g., invocation_id, updated_at) when it builds the models.
        columns_to_drop = [
            "bronze_ingested_time",
            "staged_at",
            "staging_invocation_id",
            "silver_updated_at",
            "silver_invocation_id",
        ]
        df = df.drop(columns_to_drop)
        project_root = Path(__file__).resolve().parents[1]

        seed_dir = project_root / "transform" / "seeds"
        output_path = seed_dir / "bronze_dim_company.csv"
        df.write_csv(output_path)

        logger.info(
            f"Seed file generated successfully. "
            f"Output Path: '{output_path}', Total Rows: {df.height}."
        )

    except Exception as e:
        logger.error(
            f"Failed to export silver company data to dbt seed. Error: {e}",
            exc_info=True,
        )
