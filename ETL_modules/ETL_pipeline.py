from IPython.display import display
from vnstock import *
import pandas as pd
import time
import os
import shutil
from sqlalchemy import *
from urllib.parse import quote_plus
from ETL_modules.stock_processed_sql import *
import sys
import json
import traceback


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








# tạo hàm cập nhật danh sách công ty 
def update_companies_list(engine):
    listing = Listing(source='VCI')

    # cập nhật bảng company_list
    df_listing = listing.symbols_by_industries()
    blacklist =['CK','NH','BH'] # loại bỏ cổ phiếu chứng khoán, ngân hàng, bảo hiểm
    df_listing = df_listing[~df_listing['com_type_code'].isin(blacklist)] # loại bỏ cổ phiếu trong blacklist
    hose_list = listing.symbols_by_group('HOSE').astype(str).str.upper().str.strip().to_list() # danh sách cổ phiếu sàn HOSE
    hnx_list = listing.symbols_by_group('HNX').astype(str).str.upper().str.strip().to_list() # danh sách cổ phiếu sàn HNX
    white_list = set(hose_list + hnx_list) # tập hợp cổ phiếu sàn HOSE và HNX
    df_listing = df_listing[df_listing['symbol'].astype(str).str.upper().str.strip().isin(white_list)] # chỉ giữ lại cổ phiếu trong white_list
    df_listing = df_listing.drop(['icb_name2','icb_name4','com_type_code','icb_code1','icb_code2','icb_code3','icb_code4'], axis=1)
    df_listing = df_listing.rename(columns={
        'symbol': 'Ticker',
        'organ_name': 'Company Name',
        'icb_name3': 'Industry Name',
    })

    df_listing.to_sql('temp_table', engine, schema = 'raw', if_exists='replace', index=False)  
    if inspector.has_table('companies_list', schema='analysis_data'):
        query = """
        INSERT INTO analysis_data.companies_list ("Ticker", "Company Name", "Industry Name")
        SELECT "Ticker", "Company Name", "Industry Name"
        FROM raw.temp_table
        ON CONFLICT ("Ticker")
        DO UPDATE SET
            "Company Name" = EXCLUDED."Company Name",
            "Industry Name" = EXCLUDED."Industry Name";
        """ # thêm dữ liệu vào bảng companies_list, nếu đã tồn tại thì cập nhật lại thông tin

        drop_temp_table = "DROP TABLE IF EXISTS raw.temp_table;" # xóa bảng tạm temp_table

        with engine.connect() as conn:
            conn.execute(text(query)) # Execute DML to insert/update data
            conn.execute(text(drop_temp_table)) # Drop temp_table
            conn.commit()









# Hàm cập nhật dữ liệu bảng balance_sheet trong schema raw
def update_balance_raw(engine):
    #truyền dữ liệu balance sheet
    failed_tickers_balance =[]
    balance_tickers = set()
    df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine)
    ticker_list = set(df_tickers['Ticker'].str.strip())
    if inspector.has_table('balance_sheet', schema='raw'):
        df_all_balance = pd.read_sql('SELECT "Ticker" FROM raw.balance_sheet', engine)
        balance_tickers = set(df_all_balance['Ticker'].str.strip())
    missing_tickers = set(ticker_list - balance_tickers)
    print(len(missing_tickers))

    for i, ticker in enumerate(missing_tickers):
        try: 
            df_balancesheet = balance_sheet(ticker)
            df_balancesheet = df_balancesheet.set_index(df_balancesheet.columns[0])
            df_balancesheet.loc['Ticker'] = ticker
            df_balancesheet = df_balancesheet.T
            df_balancesheet.columns.name = None
            df_balancesheet.index.name = 'year'
            df_balancesheet = df_balancesheet.reset_index()
            if 'Ticker' in df_balancesheet.columns and 'year' in df_balancesheet.columns: # đảm bảo không có dữ liệu trùng lặp
                df_balancesheet = df_balancesheet.drop_duplicates(subset= ['Ticker', 'year'], keep='last')
            if inspector.has_table('balance_sheet'):
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM raw.balance_sheet WHERE \"Ticker\" = '{ticker}'"))
                    conn.commit()
            df_balancesheet.to_sql('balance_sheet', engine, schema='raw', if_exists='append', index=False)
            print(f"Đã xử lý xong ticker {i+1}/{len(missing_tickers)}: {ticker}")
        except Exception as e:
            print(f"Lỗi với ticker {ticker}: {e}")
        time.sleep(3)  # Thêm độ trễ 5 giây giữa các lần lặp
        
    print(pd.DataFrame(failed_tickers_balance))


#truyền dữ liệu income statement raw
def update_income_raw(engine):
    df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine)
    ticker_list = set(df_tickers['Ticker'].str.strip())
    income_tickers = set()
    if inspector.has_table('income_statement', schema='raw'):
        df_all_income = pd.read_sql('SELECT "Ticker" FROM raw.income_statement', engine)
        income_tickers = set(df_all_income['Ticker'].str.strip())
    missing_tickers = set(ticker_list - income_tickers)
    print(missing_tickers), print(len(missing_tickers))
        
    failed_tickers_income =[]
    for i, ticker in enumerate(missing_tickers):
        try: 
            df_ic = finance_statement(ticker)
            df_ic = df_ic.set_index(ticker)
            df_ic.loc['Ticker'] = ticker 
            df_ic = df_ic.T
            df_ic.columns.name = None
            df_ic.index.name = 'year'
            df_ic = df_ic.reset_index()
            if 'Ticker' in df_ic.columns and 'year' in df_ic.columns: # đảm bảo không có dữ liệu trùng lặp
                df_ic = df_ic.drop_duplicates(subset= ['Ticker', 'year'], keep='last')
            if inspector.has_table('income_statement'):
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM raw.income_statement WHERE \"Ticker\" = '{ticker}'"))
                    conn.commit()
            df_ic.to_sql('income_statement', engine, schema='raw', if_exists='append', index=False)
            print(f"Đã xử lý xong ticker {i+1}/{len(missing_tickers)}: {ticker}")

        except Exception as e:
            print(f"Lỗi với ticker {ticker}: {e}")
            failed_tickers_income.append({'ticker': ticker, 'error': str(e)})
        time.sleep(3)

    print(f"tìm thấy {len(failed_tickers_income)} ticker(s) lỗi")


# Hàm cập nhật dữ liệu bảng cash_flow trong schema raw
def update_cashflow_raw(engine):
    # truyền dữ liệu cash flow
    indirect_tickers = set()
    direct_tickers = set()
    df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine)
    ticker_list = set(df_tickers['Ticker'].str.strip())
    if inspector.has_table('cash_flow_indirect', schema='raw'):
        df_all_indirect = pd.read_sql('SELECT "Ticker" FROM raw.cash_flow_indirect', engine)
        indirect_tickers = set(df_all_indirect['Ticker'].str.strip())
    if inspector.has_table('cash_flow_direct', schema='raw'):
        df_all_direct = pd.read_sql('SELECT "Ticker" FROM raw.cash_flow_direct', engine)
        direct_tickers = set(df_all_direct['Ticker'].str.strip())
    missing_tickers = list(ticker_list - (indirect_tickers | direct_tickers))
    tickers_failed_cash_flow = []
    print(missing_tickers)

    for i, ticker in enumerate(missing_tickers):
        try:
            target_table = 'unknown'
            df_cf = cash_flow_statement(ticker)
            df_cf = df_cf.set_index(df_cf.columns[0])
            df_cf.loc['Ticker'] = ticker
            df_cf = df_cf.T
            df_cf.columns.name = None
            df_cf.index.name = 'year'
            df_cf = df_cf.reset_index()
            df_cf.columns = df_cf.columns.str.strip()
            if 'Ticker' in df_cf.columns and 'year' in df_cf.columns: # đảm bảo không có dữ liệu trùng lặp
                df_cf = df_cf.drop_duplicates(subset= ['Ticker', 'year'], keep='last')
            if '1. Tiền thu từ bán hàng, cung cấp dịch vụ và doanh thu khác' in df_cf.columns:
                target_table = 'cash_flow_direct'
            elif '2. Điều chỉnh cho các khoản' in df_cf.columns:
                target_table = 'cash_flow_indirect'
            if inspector.has_table(target_table, schema='raw'):
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM raw.{target_table} WHERE \"Ticker\" = '{ticker}'"))
                    conn.commit()
            df_cf.to_sql(target_table, engine, schema='raw', if_exists='append', index=False)
            print(f"Đã xử lý xong ticker {i+1}/{len(missing_tickers)}: {ticker} vào bảng {target_table}")
        except Exception as e:
            print(f"Lỗi với ticker {ticker}: {e}")
            tickers_failed_cash_flow.append({'ticker': ticker, 'error': str(e)})
        time.sleep(1)  # Thêm độ trễ 5 giây giữa các lần lặp

    print(f" Tìm thấy {len(tickers_failed_cash_flow)} ticker(s) lỗi")

# Hàm cập nhật dữ liệu bảng financial_ratio trong schema raw
def update_ratio_raw(engine, inspector):
    failed_tickers = []
    ratio_ticker = set()
    df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine)
    ticker_list = set(df_tickers['Ticker'].str.strip())
    if inspector.has_table('financial_ratio', schema='raw'):
        df_all_ratio = pd.read_sql('SELECT "Ticker" FROM raw.financial_ratio', engine)
        ratio_ticker = set(df_all_ratio['Ticker'].str.strip())
    missing_tickers = set(ticker_list - ratio_ticker)
    print(len(missing_tickers))
    column_mapping = {
        "Nhóm chỉ số Định giá": "Valuation Ratios",
        "EPS": "eps",
        "BVPS": "bvps",
        "P/E": "pe",
        "P/B": "pb",
        "P/S": "ps",
        "EV/EBITDA": "ev_ebitda",
        "Nhóm chỉ số Sinh lợi": "Profitability Ratios",
        "Biên EBIT (%)": "ebit_margin",
        "Biên lợi nhuận gộp (%)": "gross_margin",
        "Biên lợi nhuận ròng (%)": "net_margin",
        "Tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA) (%)": "roaa",
        "Tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA) (%)": "roea",
        "Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE) (%)": "roce",
        "Nhóm chỉ số Tăng trưởng": "Growth Ratios",
        "Tăng trưởng doanh thu thuần (%)": "revenue_growth",
        "Tăng trưởng lợi nhuận sau thuế (%)": "net_income_growth",
        "Nhóm chỉ số Thanh khoản": "Liquidity Ratios",
        "Tỷ số thanh toán hiện hành (ngắn hạn)": "current_ratio",
        "Chỉ số thanh toán nhanh": "quick_ratio",
        "Khả năng chi trả lãi vay": "interest_coverage_ratio",
        "Nhóm chỉ số Hiệu quả hoạt động": "Efficiency Ratios",
        "Thời gian thu tiền khách hàng bình quân (ngày)": "days_sales_outstanding",
        "Thời gian tồn kho bình quân (ngày)": "days_inventory_outstanding",
        "Vòng quay hàng tồn kho": "inventory_turnover",
        "Vòng quay tổng tài sản": "total_asset_turnover",
        "Số ngày trả cho nhà cung cấp (ngày)": "days_payable_outstanding",
        "Vòng quay trả cho nhà cung cấp": "payable_turnover",
        "Chu kỳ chuyển đổi tiền mặt (CCC) (ngày)": "cash_conversion_cycle",
        "Nhóm chỉ số Đòn bẩy tài chính": "Financial Leverage Ratios",
        "Tỷ số Nợ vay trên Vốn chủ sở hữu (%)": "debt_to_equity_ratio",
        "Tỷ số Nợ vay trên Tổng tài sản (%)": "debt_to_assets_ratio",
        "Nhóm chỉ số Dòng tiền": "Cash Flow Ratios",
        "Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)": "operating_cash_flow_to_revenue_ratio",
        "Dòng tiền từ HĐKD trên Tổng tài sản (%)": "operating_cash_flow_to_total_assets_ratio",
        "Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)": "operating_cash_flow_to_equity_ratio",
        "Dòng tiền từ HĐKD trên mỗi cổ phần": "operating_cash_flow_per_share"
    }
    for i, ticker in enumerate(missing_tickers):
        try: 
            df_ratio = ratio_calculation(ticker)
            df_ratio = df_ratio.set_index('Chỉ số')
            df_ratio.loc['Ticker'] = ticker
            df_ratio = df_ratio.T
            df_ratio.index.name = 'year'
            df_ratio.columns.name = None
            df_ratio = df_ratio.reset_index()
            if 'Ticker' in df_ratio.columns and 'year' in df_ratio.columns:
                df_ratio = df_ratio.drop_duplicates(subset= ['Ticker', 'year'], keep='last')
            df_ratio = df_ratio.rename(columns=column_mapping)
            if inspector.has_table('financial_ratio'):
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM raw.financial_ratio WHERE TRIM(\"Ticker\") = '{ticker}'"))
                    conn.commit()
            
            df_ratio.to_sql('financial_ratio', engine ,schema = 'raw', if_exists='append', index=False)
            print(f"Đã xử lý xong ticker {i+1}/{len(missing_tickers)}: {ticker}")
        except KeyboardInterrupt:
            print("Quá trình bị gián đoạn bởi người dùng.")
            sys.exit(0)
        except Exception as e:
            print(f"Lỗi với ticker {ticker}: {e}")
            traceback.print_exc()
            failed_tickers.append({'ticker': ticker, 'error': str(e)})
        time.sleep(3)  # Thêm độ trễ 2 giây giữa các lần lặp

    display(pd.DataFrame(failed_tickers))

    if os.path.exists('data_processed'):
        shutil.rmtree('data_processed')

    if os.path.exists('data_raw'):
        shutil.rmtree('data_raw')

        
        
def insert_ic_quarter(engine): # Hàm chèn dữ liệu báo cáo tài chính quý vào bảng ic_quarter trong schema raw
    quarter_tickers = set() # Tạo set rỗng trước
    df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine) # Lấy danh sách ticker từ bảng companies_list
    ticker_list = set(df_tickers['Ticker'].str.strip()) # Chuyển danh sách ticker thành set để dễ dàng so sánh
     # Lấy danh sách ticker đã có trong bảng ic_quarter
    if inspector.has_table('ic_quarter', schema='raw'):
        df_all_quarter_ic = pd.read_sql('SELECT "Ticker" FROM raw.ic_quarter', engine)
        quarter_tickers = set(df_all_quarter_ic['Ticker'].str.strip()) # Chuyển danh sách ticker đã có thành set
    missing_tickers = set(ticker_list - quarter_tickers) # Tìm các ticker chưa có trong bảng ic_quarter
    print(len(missing_tickers))

    for i,ticker in enumerate(missing_tickers): # Lặp qua từng ticker chưa có
        try:
            print(f'Processing {i+1}/{len(missing_tickers)}: {ticker}')
            co_phieu = ticker
            start = 2020
            end = 2025
            years = range(end, start-1, -1)
            quarters = [4,3,2,1]
            auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
            user = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            headers = { 'Authorization': auth_token,
                        'User-Agent': user}
            query = text("""
                        INSERT INTO raw.ic_quarter ("Ticker", "Year", "Quarter", "data")
                        VALUES (:ticker, :year, :quarter, :data)
                        ON CONFLICT ("Ticker", "Year", "Quarter") 
                        DO UPDATE SET 
                        "data" = EXCLUDED."data",
                        "insert at" = CURRENT_TIMESTAMP;
                        
            """) # Câu lệnh chèn dữ liệu với xử lý trùng lặp
            with engine.connect() as conn:
                transaction = conn.begin()
                if inspector.has_table('ic_quarter', schema='raw'): # Kiểm tra nếu bảng ic_quarter đã tồn tại
                    try:
                        for y in years: # tạo danh sách url tự động từ năm end đến start
                            print(f'Year: {y}')
                            for q in quarters:
                                print(f'  Quarter: {q}')
                                url = f'https://restv2.fireant.vn/symbols/{co_phieu}/full-financial-reports?type=2&year={y}&quarter={q}&limit=1' # URL API
                                params = { 'type': '2', 
                                'year': str(y), 
                                'quarter': str(q), 
                                'limit': '1'}
                                response = requests.get(url, headers=headers, params=params) # Gửi yêu cầu GET
                                if response.status_code == 200:
                                    data = response.json() # đổi thành object trong python
                                    if not data:
                                        continue
                                    resp_json = json.dumps(data, ensure_ascii=False) # Chuyển object thành chuỗi JSON
                                    conn.execute(query, {"ticker": co_phieu, "year": y, "quarter": q, "data": resp_json}) # Thực thi câu lệnh chèn dữ liệu
                                time.sleep(1) # Thời gian chờ giữa các yêu cầu để tránh bị chặn
                        print(f'xử lý xong cho {co_phieu}')
                        transaction.commit()
                    except Exception as e:
                        transaction.rollback()
                        print(f"Error: {e}")
                        traceback.print_exc()
        except Exception as e:
            print(f"Failed to process {ticker}: {e}")
            traceback.print_exc()

# Hàm cập nhật dữ liệu giá cổ phiếu hàng ngày với cơ chế thông minh (smart catch-up)
def daily_catchup_update(engine):
    print("--- 🔄 CHẠY CẬP NHẬT THÔNG MINH (SMART CATCH-UP) ---")
    header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
    }
    
    with engine.connect() as conn:
        transaction = conn.begin()
        # 1. Tìm ngày mới nhất đang có trong kho
        start_date = conn.execute(text('SELECT MAX("date") FROM raw.daily_price_history')).scalar()

        if start_date is None:
            print("⚠️ Kho trống! Vui lòng chạy tải dữ liệu đầy đủ ban đầu trước khi chạy cập nhật thông minh.")
            return
        # 2. Tính toán khoảng thời gian cần bù
        end_date = datetime.now().date()
        if start_date == end_date: 
            print("✅ Dữ liệu đã mới nhất.")
            return

        if start_date < end_date:
            sql_upsert = text("""
            INSERT INTO raw.daily_price_history ("Ticker", open, high, close, volume, date)
            VALUES (:ticker, :open, :high, :close, :volume, :date)
            ON CONFLICT ("Ticker", "date") 
            DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, close = EXCLUDED.close, volume = EXCLUDED.volume
                              """)
            # 3. Lấy danh sách ticker từ bảng companies_list
            df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine)
            ticker_list = set(df_tickers['Ticker'].str.strip())
            # 4. Lặp qua từng ticker và tải dữ liệu cần bù
            for i,ticker in enumerate(ticker_list):
                print(f"⏳ [{i+1}/{len(ticker_list)}] Đang cập nhật {ticker} từ {start_date} đến {end_date}...")
                # Gọi API để lấy dữ liệu từ start_date đến end_date
                api_url = f"https://restv2.fireant.vn/symbols/{ticker}/historical-quotes?startDate={start_date}&endDate={end_date}&offset=0&limit=30"
                params = {'startDate': start_date, 'endDate': end_date, 'offset': 0, 'limit': 30}
                response = requests.get(api_url, headers=header, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for record in data:
                        date = record.get('date').split('T')[0]  # Lấy phần ngày, bỏ phần thời gian
                         # Chèn hoặc cập nhật dữ liệu vào bảng daily_price_history
                        conn.execute(sql_upsert, {
                            'ticker': ticker,
                            'open': record.get('priceOpen'),
                            'high': record.get('priceHigh'),
                            'close': record.get('priceClose'),
                            'volume': record.get('volume'),
                            'date': date
                        })
                        time.sleep(0.2)  # Giữ rate limit
                time.sleep(0.3)  # Giữ rate limit giữa các ticker
        transaction.commit()   
        
        
# if __name__ == "__main__":
    # update_balance_raw(engine)
    # update_income_raw(engine)
    # update_cashflow_raw(engine)
    # update_ratio_raw(engine)