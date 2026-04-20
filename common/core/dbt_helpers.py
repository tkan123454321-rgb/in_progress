
from dotenv import load_dotenv
import requests
from pathlib import Path
from common.clients.lakehouse_client import LakeHouseClient
from common.core.logger_config import setup_logger
logger = setup_logger(component="utils")

def export_silver_company_to_seed():
    """
    Extracts data from the `silver.silver_dim_company` table and exports it to a CSV file for dbt seed.
    This serves as a reliable backup source in case the original table or API fails.
    """
    try:
        client = LakeHouseClient()
        table_path = "silver.silver_dim_company" 
        logger.info(f"🚀 Đang hút dữ liệu từ {table_path}...")
        table = client.catalog.load_table(table_path)
        df = table.scan().to_polars()
        columns_to_drop = [
            "bronze_ingested_time",
            "staged_at", 
            "staging_invocation_id", 
            "silver_updated_at", 
            "silver_invocation_id"
        ]
        df = df.drop(columns_to_drop)
        project_root = Path(__file__).resolve().parents[1]
        
        # Trỏ thẳng vào thư mục seeds của dbt
        seed_dir = project_root / "transform" / "seeds"
        output_path = seed_dir / "bronze_dim_company.csv"
        # 5. Ghi file CSV
        df.write_csv(output_path)
        
        logger.info(f"✅ Đã xuất {df.height} dòng ra {output_path} thành công! Đã sẵn sàng cho dbt seed!")
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi xuất dữ liệu làm seed: {str(e)}")