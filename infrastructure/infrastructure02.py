from vnstock import *
import pandas as pd
import time
import os
import shutil
from sqlalchemy import *
from urllib.parse import quote_plus
import requests
import json
from datetime import datetime
# Database connection setup
db_user = 'tkan'
db_password = 'Maihainganha@1'
db_host = 'localhost'  
db_port = '5432'       
db_name = 'finance_db' 
password = quote_plus(db_password) # Encode password
connection_str = f'postgresql://{db_user}:{password}@{db_host}:{db_port}/{db_name}' # Connection string
engine = create_engine(connection_str) # Create engine
inspector = inspect(engine) # Create inspector

def init_db_infrastructure(engine): # hàm tạo cơ sở hạ tầng database
    with engine.connect() as connection:
        
        
        # tạo schema nếu chưa có 
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analysis_data;"))
        
        
        
        #tạo bảng master (company_list) nếu chưa có)
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS analysis_data.companies_list (
         "Ticker" VARCHAR(10) PRIMARY KEY, 
        "Company Name" VARCHAR(300),
         "Industry Name" VARCHAR(200)
        );
        """))
        
        # tạo bảng cho lưu chuyển tiền tệ gián tiếp theo năm nếu chưa có
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.cash_flow_indirect_year (
            year INTEGER NOT NULL,
            "Ticker" VARCHAR(10) NOT NULL,
            "profit_before_tax" BIGINT,                 -- 1. Lợi nhuận trước thuế
            -- Các khoản điều chỉnh
            "depreciation_amortization" BIGINT,         -- Khấu hao TSCĐ
            "provisions" BIGINT,                        -- Các khoản dự phòng
            "profit_from_associates" BIGINT,            -- Lợi nhuận từ cty liên kết (Thường là trừ ra)
            "write_off_fixed_assets" BIGINT,            -- Xóa sổ TSCĐ
            "unrealized_fx_gain_loss" BIGINT,           -- Lãi/lỗ chênh lệch tỷ giá chưa thực hiện
            "gain_loss_disposal_fixed_assets" BIGINT,   -- Lãi/lỗ thanh lý TSCĐ
            "investment_gain_loss" BIGINT,              -- Lãi/lỗ từ hoạt động đầu tư
            "deposit_interest_income" BIGINT,           -- Lãi tiền gửi
            "interest_income" BIGINT,                   -- Thu nhập lãi (chung)
            "interest_expense" BIGINT,                  -- Chi phí lãi vay (Điều chỉnh)
            "direct_deductions_from_profit" BIGINT,     -- Các khoản chi trực tiếp từ lợi nhuận
            
            "operating_profit_before_wc_change" BIGINT, -- 3. Lợi nhuận KD trước thay đổi vốn lưu động
            -- Thay đổi vốn lưu động
            "chg_receivables" BIGINT,                   -- Tăng/giảm phải thu
            "chg_inventory" BIGINT,                     -- Tăng/giảm tồn kho
            "chg_payables" BIGINT,                      -- Tăng/giảm phải trả (trừ lãi, thuế)
            "chg_prepaid_expenses" BIGINT,              -- Tăng/giảm chi phí trả trước
            "chg_other_current_assets" BIGINT,          -- Tăng/giảm tài sản NH khác
            
            -- Dòng tiền thực tế
            "interest_paid" BIGINT,                     -- Tiền lãi vay đã trả
            "income_tax_paid" BIGINT,                   -- Thuế TNDN đã nộp
            "other_cash_inflow_operating" BIGINT,       -- Tiền thu khác từ HĐKD
            "other_cash_outflow_operating" BIGINT,      -- Tiền chi khác từ HĐKD
            
            "net_cash_operating" BIGINT,                -- LƯU CHUYỂN TIỀN THUẦN TỪ HĐKD (Key)

            -- II. LƯU CHUYỂN TIỀN TỪ HOẠT ĐỘNG ĐẦU TƯ (CFI)
            "payments_for_fixed_assets" BIGINT,         -- 1. Tiền chi mua sắm, xây dựng TSCĐ (Capex)
            "proceeds_disposal_fixed_assets" BIGINT,    -- 2. Tiền thu thanh lý, nhượng bán TSCĐ
            "loans_to_others" BIGINT,                   -- 3. Tiền chi cho vay, mua công cụ nợ
            "proceeds_loan_collections" BIGINT,         -- 4. Tiền thu hồi cho vay
            "invest_in_associates" BIGINT,              -- 5. Đầu tư góp vốn vào cty liên kết
            "st_investments_payments" BIGINT,           -- 6. Chi đầu tư ngắn hạn
            "equity_investments_payments" BIGINT,       -- 7. Tiền chi đầu tư góp vốn đơn vị khác
            "proceeds_equity_disposals" BIGINT,         -- 8. Tiền thu hồi đầu tư góp vốn
            "interest_received_deposit" BIGINT,         -- 9. Lãi tiền gửi đã thu
            "interest_dividends_received" BIGINT,       -- 10. Tiền thu lãi cho vay, cổ tức
            "payments_minority_interest_shares" BIGINT, -- 11. Chi mua lại vốn góp của CĐ thiểu số
            
            "net_cash_investing" BIGINT,                -- LƯU CHUYỂN TIỀN THUẦN TỪ HĐ ĐẦU TƯ (Key)

            -- III. LƯU CHUYỂN TIỀN TỪ HOẠT ĐỘNG TÀI CHÍNH (CFF)
            "proceeds_share_issuance" BIGINT,           -- 1. Thu từ phát hành cổ phiếu
            "payments_share_repurchase" BIGINT,         -- 2. Chi trả vốn góp, mua lại cổ phiếu (Treasury)
            "proceeds_borrowings" BIGINT,               -- 3. Tiền vay nhận được (Gốc)
            "repayment_borrowings" BIGINT,              -- 4. Chi trả nợ gốc vay
            "finance_lease_payments" BIGINT,            -- 5. Chi trả nợ thuê tài chính
            "other_cash_outflow_financing" BIGINT,      -- 6. Tiền chi khác từ HĐTC
            "equitization_payments" BIGINT,             -- 7. Tiền chi trả từ cổ phần hóa
            "dividends_paid" BIGINT,                    -- 8. Cổ tức, lợi nhuận đã trả
            "minority_capital_contribution" BIGINT,     -- 9. Vốn góp của CĐ thiểu số
            "social_welfare_fund_spending" BIGINT,      -- 10. Chi tiêu quỹ phúc lợi xã hội
            
            "net_cash_financing" BIGINT,                -- LƯU CHUYỂN TIỀN THUẦN TỪ HĐ TÀI CHÍNH (Key)

            -- TỔNG HỢP
            "net_change_in_cash" BIGINT,                -- Lưu chuyển tiền thuần trong kỳ
            "cash_begin" BIGINT,                        -- Tiền đầu kỳ
            "fx_impact" BIGINT,                         -- Ảnh hưởng tỷ giá
            "cash_end" BIGINT,
            CONSTRAINT pk_cash_flow_indirect_year PRIMARY KEY ("Ticker", year),
            CONSTRAINT fk_cash_flow_indirect_year_ticker FOREIGN KEY ("Ticker") 
                REFERENCES analysis_data.companies_list("Ticker") 
                    ON DELETE 
                        CASCADE,
            CONSTRAINT chk_cash_flow_indirect_year CHECK (year >= 2018 AND year <= EXTRACT(YEAR FROM CURRENT_DATE))
            );
            """))
        
        
        
        # tạo bảng báo cáo lưu chuyển tiền tệ trực tiếp theo năm nếu chưa có
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.cash_flow_direct_year (
            year INTEGER NOT NULL,
            "Ticker" VARCHAR(10) NOT NULL,
            "receipts_from_customers" BIGINT,       -- 1. Tiền thu từ bán hàng
            "payments_to_suppliers" BIGINT,         -- 2. Tiền chi trả người cung cấp
            "payments_to_employees" BIGINT,         -- 3. Tiền chi trả người lao động
            "interest_paid" BIGINT,                 -- 4. Tiền chi trả lãi vay
            "corporate_tax_paid" BIGINT,            -- 5. Tiền chi nộp thuế TNDN
            "vat_tax_paid" BIGINT,                  -- 6. Tiền chi nộp thuế GTGT (Đặc thù)
            "other_operating_receipts" BIGINT,      -- 7. Tiền thu khác
            "other_operating_payments" BIGINT,      -- 8. Tiền chi khác
            
            "net_cash_operating" BIGINT,            -- LƯU CHUYỂN TIỀN THUẦN TỪ HĐKD (Key)

            -- II. LƯU CHUYỂN TIỀN TỪ HOẠT ĐỘNG ĐẦU TƯ (CFI)
            "payments_for_fixed_assets" BIGINT,     -- Chi mua sắm TSCĐ
            "proceeds_disposal_fixed_assets" BIGINT,-- Thu thanh lý TSCĐ
            "loans_to_others" BIGINT,               -- Chi cho vay
            "proceeds_loan_collections" BIGINT,     -- Thu hồi cho vay
            "equity_investments" BIGINT,            -- Đầu tư góp vốn
            "proceeds_equity_disposals" BIGINT,     -- Thu hồi đầu tư vốn
            "interest_dividends_received" BIGINT,   -- Thu lãi, cổ tức
            
            "net_cash_investing" BIGINT,            -- LƯU CHUYỂN TIỀN THUẦN TỪ HĐ ĐẦU TƯ

            -- III. LƯU CHUYỂN TIỀN TỪ HOẠT ĐỘNG TÀI CHÍNH (CFF)
            "proceeds_share_issuance" BIGINT,       -- Thu từ phát hành cổ phiếu
            "payments_share_repurchase" BIGINT,     -- Chi mua lại cổ phiếu
            "proceeds_borrowings" BIGINT,           -- Tiền vay nhận được
            "repayment_borrowings" BIGINT,          -- Chi trả nợ gốc vay
            "financing_payments_for_assets" BIGINT,
            "finance_lease_payments" BIGINT,        -- Chi trả nợ thuê tài chính
            "dividends_paid" BIGINT,                -- Cổ tức đã trả
            "other_financing_payments" BIGINT,      -- Chi từ các quỹ (như trong ảnh)
            
            "net_cash_financing" BIGINT,            -- LƯU CHUYỂN TIỀN THUẦN TỪ HĐ TÀI CHÍNH

            -- TỔNG HỢP
            "net_change_in_cash" BIGINT,            -- Lưu chuyển tiền thuần trong kỳ
            "cash_begin" BIGINT,                    -- Tiền đầu kỳ
            "fx_impact" BIGINT,                     -- Ảnh hưởng tỷ giá
            "cash_end" BIGINT,
            CONSTRAINT pk_cash_flow_direct_year PRIMARY KEY ("Ticker", year),
            CONSTRAINT fk_cash_flow_direct_year_ticker FOREIGN KEY ("Ticker") 
                REFERENCES analysis_data.companies_list("Ticker") 
                    ON DELETE 
                        CASCADE,
            CONSTRAINT chk_cash_flow_direct_year CHECK (year >= 2018 AND year <= EXTRACT(YEAR FROM CURRENT_DATE))
            );
            """))
        
        
        
        # tạo bảng báo cáo kết quả kinh doanh theo quý nếu chưa có
        connection.execute(text("""
        
        CREATE TABLE IF NOT EXISTS raw.ic_quarter (
            "Ticker" VARCHAR(10) NOT NULL,
            "Year" INTEGER NOT NULL,
            "Quarter" INTEGER NOT NULL,
            "data" JSONB,
            "insert at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT PK_ic_quarter PRIMARY KEY ("Ticker", "Year", "Quarter"),
        CONSTRAINT fk_ic_quarter_ticker FOREIGN KEY ("Ticker") 
            REFERENCES analysis_data.companies_list("Ticker") 
            ON DELETE 
                CASCADE,
        CONSTRAINT chk_ic_quarter_year CHECK ("Year" >= 2019 AND "Year" <= EXTRACT(YEAR FROM CURRENT_DATE)),
        CONSTRAINT chk_ic_quarter_quarter CHECK ("Quarter" >= 1 AND "Quarter" <= 4)
        );
        """))
        connection.commit()



def data_clean_sql(engine, inspector): # hàm dọn dẹp dữ liệu trong các bảng
    with engine.connect() as connection:
        transaction = connection.begin()
        try: 
            if inspector.has_table('companies_list', schema='analysis_data'): # chuẩn hóa dữ liệu bảng companies_list
                connection.execute(text("""
                UPDATE analysis_data.companies_list
                SET "Ticker" = UPPER(TRIM("Ticker")),
                    "Company Name" = LOWER(TRIM("Company Name")),
                    "Industry Name" = LOWER(TRIM("Industry Name"))
                """))
                
            tables = ['balance_sheet', 'income_statement', 'cash_flow_direct', 'financial_ratio']
            for table in tables:
                if inspector.has_table(table, schema='raw'): # xóa các bản ghi trùng lặp trong các bảng dữ liệu tài chính nếu có
                    connection.execute(text(f"""
                    ALTER TABLE raw.{table} 
                    ALTER COLUMN year TYPE INTEGER USING year::INTEGER;
                    
                    DELETE FROM raw.{table} a
                    USING raw.{table} b
                    WHERE a."Ticker" = b."Ticker"
                        AND a.year = b.year
                        AND a.ctid < b.ctid;
                    
                    UPDATE raw.{table} -- chuẩn hóa dữ liệu trong các bảng tài chính
                    SET "Ticker" = UPPER(TRIM("Ticker")),
                        year = CAST(year AS INTEGER); 

                    DELETE FROM raw.{table} -- xóa các bản ghi có year null hoặc year < 2000 hoặc year > current year
                    WHERE year IS NULL OR year < 2019 OR year > EXTRACT(YEAR FROM CURRENT_DATE);
                    """))
            # chuẩn hoá bảng financial_ratio với các điều kiện cụ thể
            cols01 = ['eps','bvps','pe','pb','ps','ev_ebitda','ebit_margin','gross_margin','net_margin','current_ratio','quick_ratio','interest_coverage_ratio','operating_cash_flow_per_share','operating_cash_flow_to_equity_ratio','operating_cash_flow_to_total_assets_ratio','operating_cash_flow_to_revenue_ratio'] # chuẩn hoá với điều kiện 0,âm vô cực, dương vô cực là null
            for col in cols01:
                connection.execute(text(f"""
                UPDATE raw.financial_ratio
                SET "{col}" = NULL
                WHERE "{col}" = 0 
                       OR CAST("{col}" AS TEXT) IN ('inf', '-inf', 'NaN', 'nan', 'None', 'Infinity', '-Infinity');
                """))
            cols02 = ['roaa','roea','roce','revenue_growth','net_income_growth','days_payable_outstanding','payable_turnover','cash_conversion_cycle'] # chuẩn hoá với điều kiện âm vô cực, dương vô cực là null, dữ liệu 2019 nếu là 0 = null
            cols03 = ['days_sales_outstanding','days_inventory_outstanding','inventory_turnover','total_asset_turnover','debt_to_equity_ratio','debt_to_assets_ratio']# chuẩn hoá với điều kiện âm vô cực, dương vô cực là null
            for col in cols02:
                connection.execute(text(f"""
                UPDATE raw.financial_ratio
                SET "{col}" = NULL
                WHERE CAST("{col}" AS TEXT) IN ('inf', '-inf', 'NaN', 'nan', 'None', 'Infinity', '-Infinity')
                       OR (year = 2019 AND "{col}" = 0);
                """))
            for col in cols03:
                connection.execute(text(f"""
                UPDATE raw.financial_ratio
                SET "{col}" = NULL
                WHERE CAST("{col}" AS TEXT) IN ('inf', '-inf', 'NaN', 'nan', 'None', 'Infinity', '-Infinity');
                """))

            
            transaction.commit() 
        except Exception as e:
            transaction.rollback()
            print(f"Error during data cleaning: {e}")

# 1. Hàm phụ trợ: Kiểm tra xem bảng đã có Primary Key chưa
def check_pk_exists(conn, table_name, schema='raw'):
    query = text(f"""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_type = 'PRIMARY KEY';
    """)
    return conn.execute(query).fetchone() is not None

# 2. Hàm phụ trợ 2: Kiểm tra xem bảng đã có Foreign Key chưa
def check_fk_exists(connection, table_name, schema='raw'):
    query = text(f"""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_type = 'FOREIGN KEY';
    """)
    return connection.execute(query).fetchone() is not None
# hàm kiểm tra check constraint tồn tại chưa
def check_check_exists(connection, table_name, schema='raw'):
    query = text(f"""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_type = 'CHECK';
    """)
    return connection.execute(query).fetchone() is not None

# 3. Hàm chính: Áp dụng các ràng buộc dữ liệu cho các bảng
def apply_constraints(engine):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # Thêm các ràng buộc vào bảng 
            tables = ['balance_sheet', 'income_statement', 'cash_flow_direct', 'financial_ratio']
            for table in tables:
                if not check_pk_exists(connection, table): # nếu chưa có PK thì thêm PK, còn có rồi thì bỏ qua
                    connection.execute(text(f"""
                        ALTER TABLE raw.{table}
                        ADD CONSTRAINT PK_{table}
                        PRIMARY KEY ("Ticker", year);
                    """))
                if not check_fk_exists(connection, table): # nếu chưa có FK thì thêm FK, còn có rồi thì bỏ qua
                    connection.execute(text(f"""
                    ALTER TABLE raw.{table} 
                    ADD CONSTRAINT fk_{table}_ticker
                    FOREIGN KEY ("Ticker")
                    REFERENCES analysis_data.companies_list("Ticker")
                    ON DELETE CASCADE;
                    """))
                if not check_check_exists(connection, table): # nếu chưa có CHECK thì thêm CHECK, còn có rồi thì bỏ qua
                    connection.execute(text(f"""
                    ALTER TABLE raw.{table}
                    ADD CONSTRAINT chk_{table}_year
                    CHECK (year >= 2019 AND year <= EXTRACT(YEAR FROM CURRENT_DATE));
                    """))
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f"Error applying constraints: {e}")
            
# Hàm chèn dữ liệu giá cổ phiếu hàng ngày vào bảng daily_price trong schema raw
def insert_daily_price(engine,start_date = '2020-01-01'):
    #bước 1: set up bảng rỗng với các khoá
    with engine.connect() as connection:
        try:
            transaction = connection.begin()
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.daily_price_jsonb (
                    "Ticker" VARCHAR(10) NOT NULL,
                    "data" JSONB,
                    "insert at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT PK_daily_price PRIMARY KEY ("Ticker"),
                    CONSTRAINT fk_daily_price_ticker FOREIGN KEY ("Ticker") 
                        REFERENCES analysis_data.companies_list("Ticker") 
                        ON DELETE 
                            CASCADE
                );
            """)) # Tạo bảng daily_price_jsonb nếu chưa tồn tại
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.daily_price_history (
                    "Ticker" VARCHAR(10) NOT NULL,
                    "open" FLOAT,
                    "high" FLOAT,
                    "low" FLOAT,
                    "close" FLOAT,
                    "volume" BIGINT,
                    "date" DATE NOT NULL,
            CONSTRAINT PK_daily_price_history PRIMARY KEY ("Ticker", "date"),
            CONSTRAINT fk_daily_price_history_ticker FOREIGN KEY ("Ticker") 
                REFERENCES analysis_data.companies_list("Ticker") 
                    ON DELETE 
                        CASCADE
                );
            """)) # Tạo bảng daily_price_history nếu chưa tồn tại
            #bước 2: nạp dữ liệu vào bảng temp daily_price_jsonb
            daily_price_tickers = set()
            df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine) # lấy danh sách ticker từ bảng companies_list
            ticker_list = set(df_tickers['Ticker'].str.strip())
            if inspector.has_table('daily_price_jsonb', schema='raw'):
                df_all_daily_price = pd.read_sql('SELECT "Ticker" FROM raw.daily_price', engine)
                daily_price_tickers = set(df_all_daily_price['Ticker'].str.strip()) # lấy danh sách ticker từ bảng daily_price
            missing_tickers = set(ticker_list - daily_price_tickers) # tìm các ticker chưa có trong bảng daily_price
            print(len(missing_tickers))
            sql_insert = text("""
                            INSERT INTO raw.daily_price_jsonb ( "Ticker", data)
                            VALUES (:ticker, :data)
                            ON CONFLICT ( "Ticker") DO UPDATE SET data = EXCLUDED.data;
                            """) # SQL insert statement
            auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
            user = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            headers = { 'Authorization': auth_token,
                                'User-Agent': user}
            today = datetime.now().strftime('%Y-%m-%d')
            params = { 'startDate': start_date, 'endDate': today, 'offset':0 } # Common parameters
            for i, co_phieu in enumerate(missing_tickers): # vòng lặp insert các ticker thiếu vào bảng daily_price_jsonb
                print(f"Processing {i+1}/{len(missing_tickers)}: {co_phieu}")
                url = f"https://restv2.fireant.vn/symbols/{co_phieu}/historical-quotes?startDate={start_date}&endDate={today}&offset=0&limit=2000"
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                sql_params = {
                            'ticker': co_phieu,
                            'data': json.dumps(data, ensure_ascii=False)
                        }
                connection.execute(sql_insert, sql_params)
                time.sleep(0.5)
            # bươc 3: chuyển dữ liệu từ bảng daily_price_jsonb sang bảng daily_price_history
            sql_flatten = text("""
                INSERT INTO raw.daily_price_history ("Ticker", "open", "high", "low", "close", "volume","date")
                SELECT 
                    "Ticker",
                    (raw_data->>'priceOpen')::FLOAT,
                    (raw_data->>'priceHigh')::FLOAT,
                    (raw_data->>'priceLow')::FLOAT,
                    (raw_data->>'priceClose')::FLOAT,
                    (raw_data->>'totalVolume')::NUMERIC::BIGINT,
                    (raw_data->>'date')::DATE
                FROM raw.daily_price_jsonb,
                    jsonb_array_elements(data) AS raw_data
                ON CONFLICT ("Ticker", "date") DO NOTHING;
                """)
            connection.execute(sql_flatten)
            #xoá bảng temp daily_price_jsonb
            connection.execute(text("DROP TABLE IF EXISTS raw.daily_price_jsonb;"))
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f"Error occurred: {e}")

if __name__ == "__main__":
    init_db_infrastructure(engine)