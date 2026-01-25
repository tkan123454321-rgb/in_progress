from sqlalchemy import text
from infrastructure.common import *



engine = get_db_engine()
 #tạo bảng báo cáo kết quả kinh doanh theo năm nếu chưa có
def create_ic_table(engine):
    table_income_statement = ["ic_year","ic_quarter"]
    with engine.begin() as connection:
        for table in table_income_statement:
            year_limit = 2018 if table == "ic_year" else 2020
            connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS raw.{table} (
                year INTEGER NOT NULL,
                "Ticker" VARCHAR(10) NOT NULL,
                "gross_revenue" BIGINT NOT NULL,
                "revenue_deductions" BIGINT,
                "net_revenue" BIGINT NOT NULL,
                "cogs" BIGINT,
                "gross_profit" BIGINT,
                "financial_income" BIGINT,
                "financial_expenses" BIGINT,
                "interest_expenses" BIGINT,
                "joint_venture_pl" BIGINT,
                "selling_expenses" BIGINT,
                "admin_expenses" BIGINT,
                "operating_profit" BIGINT,
                "other_income" BIGINT,
                "other_expenses" BIGINT,
                "other_profit" BIGINT,
                "profit_before_tax" BIGINT NOT NULL,
                "tax_current" BIGINT,
                "tax_deferred" BIGINT,
                "profit_after_tax" BIGINT,
                "minority_interest" BIGINT,
                "net_income_parent" BIGINT,
        
        CONSTRAINT PK_{table} PRIMARY KEY ("Ticker", year),
        CONSTRAINT fk_{table} FOREIGN KEY ("Ticker") 
            REFERENCES analysis_data.companies_list("Ticker") 
            ON DELETE 
                CASCADE,
        CONSTRAINT chk_{table} CHECK (year >= {year_limit} AND year <= EXTRACT(YEAR FROM CURRENT_DATE))
            );
        """))
