import time
import pandas as pd
import os
from sqlalchemy import *
from urllib.parse import quote_plus
import numpy as np
import sys
import traceback
from infrastructure.infrastructure02 import *
import requests
import json
import random
from infrastructure.common import *
from pathlib import Path
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading





engine = get_db_engine()
inspector = inspect(engine)

# hàm lấy báo cáo tài chính
def finance_statement(stock):
    finace_data = Finance(symbol=stock, source="VCI")
    finance = finace_data.income_statement(period='year', lang='vi')  # lấy dữ liệu báo cáo tài chính
    # tạo framework chuẩn
    finance_khoiphuc = finance.set_index('Năm').T #transpose data, set header chuẩn
    df_finance = finance_khoiphuc.reset_index().rename(columns = {'index':stock})

    # # xoá rows xấu, ko cần thiết
    df_final = df_finance[~df_finance[stock].isin([
        'CP',
        'Tăng trưởng doanh thu (%)',
        'Doanh thu (đồng)',
        'Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)',
        'Tăng trưởng lợi nhuận (%)',
        'Lãi lỗ trong công ty liên doanh, liên kết'
        ])]

    # đổi tên các row cho đúng formal
    df_đổi_tên = {
        'Doanh thu thuần': 'Doanh thu thuần về bán hàng và cung cấp dịch vụ',
        'Lãi gộp': 'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
        'Thu nhập tài chính': 'Doanh thu hoạt động tài chính',
        'Lãi/Lỗ từ hoạt động kinh doanh': 'Lợi nhuận thuần từ hoạt động kinh doanh',
        'Thu nhập/Chi phí khác': 'Chi phí khác',
        'LN trước thuế': 'Tổng lợi nhuận kế toán trước thuế',
        'Lợi nhuận thuần': 'Lợi nhuận sau thuế thu nhập doanh nghiệp',
        'Cổ đông thiểu số': 'Lợi ích của cổ đông thiểu số',
        'Cổ đông của Công ty mẹ': 'Lợi nhuận sau thuế của cổ đông của Công ty mẹ',
        'Chi phí tiền lãi vay': 'Trong đó: chi phí tiền lãi vay'
    }
    df_final[stock] = df_final[stock].replace(df_đổi_tên)

    # sắp xếp các row cho đúng vị trí
    df_thutudung = [
        'Doanh thu bán hàng và cung cấp dịch vụ',
        'Các khoản giảm trừ doanh thu',
        'Doanh thu thuần về bán hàng và cung cấp dịch vụ',
        'Giá vốn hàng bán',
        'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
        'Doanh thu hoạt động tài chính',
        'Chi phí tài chính',
        'Trong đó: chi phí tiền lãi vay',
        'Lãi/lỗ từ công ty liên doanh',
        'Chi phí bán hàng',
        'Chi phí quản lý DN',
        'Lợi nhuận thuần từ hoạt động kinh doanh',
        'Thu nhập khác',
        'Chi phí khác',
        'Lợi nhuận khác',
        'Tổng lợi nhuận kế toán trước thuế',
        'Chi phí thuế TNDN hiện hành',
        'Chi phí thuế TNDN hoãn lại',
        'Lợi nhuận sau thuế thu nhập doanh nghiệp',
        'Lợi ích của cổ đông thiểu số',
        'Lợi nhuận sau thuế của cổ đông của Công ty mẹ'
    ]
    df_final = df_final.set_index(stock).reindex(df_thutudung).reset_index()

    #xoá các cột ko cần thiết
    columns_to_drop = [str(column) for column in range(2013, 2018)]
    df_final.columns = [str(c).strip() for c in df_final.columns] # loại bỏ khoảng trắng thừa ở tên cột
    current_columns = df_final.columns.tolist() # danh sách cột hiện có trong DataFrame

    columns_exist_to_drop = []
    for col in columns_to_drop:
        if col in current_columns:
            columns_exist_to_drop.append(col)
    df_final = df_final.drop(columns=columns_exist_to_drop, errors='ignore')
    # đổi lại thứ tự các cột từ 2019-2024
    year_today = time.localtime().tm_year
    right_order = [str(year) for year in range(2018, year_today + 1)]
    available_year = []
    for year in right_order:
        if year in df_final.columns:
            available_year.append(year)
    df_final = df_final[[stock] + available_year]
    df_final = df_final.fillna(0)
    return df_final
  
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
        
def _SQL_for_bl_quarter():
    
    balance_sheet_mapping = {"101" : "current_assets","10101": "cash_and_equivalents","1010101": "cash","1010102":    "cash_equivalents","10102":    "st_investments","1010201":    "st_trading_securities","1010202":    "st_trading_securities_provision","1010203":    "st_held_to_maturity","10103":    "st_receivables","1010301":    "st_receivables_customers","1010302":    "st_prepayments_to_suppliers","1010303":    "st_receivables_internal","1010304":    "st_receivables_construction","1010305":    "st_receivables_loans","1010306":    "st_receivables_other","1010307":    "st_bad_debt_provision","10104":    "inventory_net","1010401":    "inventory_gross","1010402":    "inventory_provision","10105":    "other_current_assets","1010501":    "st_prepaid_expenses","1010502":    "deductible_vat","1010503":    "tax_receivables","1010504":    "st_govt_bonds_repo", "1010505":    "st_other_assets_items" ,"102":    "non_current_assets","10201":    "lt_receivables","1020101":    "lt_receivables_customers","1020102":    "lt_capital_in_subordinates","1020103":    "lt_receivables_internal","1020104":    "lt_receivables_loans","1020105":    "lt_receivables_other","1020106":    "lt_bad_debt_provision","10202":    "fixed_assets","1020201":    "tangible_fixed_assets","102020101":    "tangible_cost","102020102":    "tangible_accum_depr","1020202":    "finance_lease_assets","102020201":    "finance_lease_cost","102020202":    "finance_lease_accum_depr","1020203":    "intangible_assets","102020301":    "intangible_cost","102020302":    "intangible_accum_depr","10203":    "investment_property","1020301":    "investment_property_cost","1020302":    "investment_property_accum_depr","10204":    "lt_work_in_progress_assets","1020401":    "lt_work_in_progress_business","1020402":    "construction_in_progress","10205":    "lt_investments","1020501":    "invest_in_subsidiaries","1020502":    "invest_in_associates","1020503":    "invest_in_other_entity","1020504":    "lt_invest_provision","1020505":    "lt_held_to_maturity","10206":    "other_non_current_assets","1020601":    "lt_prepaid_expenses","1020602":    "deferred_tax_assets","1020603":    "lt_other_assets_items","10207":    "goodwill","2":    "total_assets","301":    "liabilities","30101":    "current_liabilities","3010101":    "st_borrowings","3010102":    "current_portion_lt_debt","3010103":    "st_payables_suppliers","3010104":    "st_advances_from_customers","3010105":    "taxes_payable","3010106":    "payables_employees","3010107":    "st_accrued_expenses","3010108":    "st_payables_internal","3010109":    "payables_construction_progress","3010110":    "st_unearned_revenue","3010111":    "st_payables_other","3010112":    "st_provisions","3010113":    "bonus_welfare_fund","3010114":    "price_stabilization_fund","3010115":    "st_govt_bonds_repo_liab","30102":    "non_current_liabilities","3010201":    "lt_payables_suppliers","3010202":    "lt_accrued_expenses","3010203":    "lt_payables_internal_capital","3010204": "lt_payables_internal","3010205":    "lt_payables_other","3010206":    "lt_borrowings","3010207":    "convertible_bonds","3010208":    "deferred_tax_liabilities","3010209":    "provision_severance_allowance","3010210":    "lt_provisions","3010211":    "lt_unearned_revenue","3010212":    "science_tech_fund","302":    "owners_equity_group","30201":    "owners_equity","3020101":    "share_capital","3020102":    "share_premium","3020103":    "convertible_bond_option","3020104":    "other_capital","3020105":    "treasury_shares","3020106":    "asset_revaluation_reserve","3020107":    "fx_reserve","3020108":    "development_fund","3020109":    "financial_reserve_fund","3020110":    "other_funds_equity","3020111":    "retained_earnings","302011101":    "retained_earnings_accum","302011102":    "retained_earnings_current","3020112":    "capital_construction_fund","3020113":    "enterprise_arrangement_fund","3020114":    "minority_interest","30202":    "other_funds_group","3020201":    "budget_sources","3020202":    "budget_sources_fixed_assets","4":    "total_resources"
                            }
    sql_lines_list = []
    for id, ten_cot_moi in balance_sheet_mapping.items():
        dong_sql = f"MAX(CASE WHEN id = '{id}' THEN item_value END) AS \"{ten_cot_moi}\""
        # Thêm vào danh sách
        sql_lines_list.append(dong_sql)

    # Nối tất cả các dòng lại với nhau bằng dấu phẩy và xuống dòng cho đẹp
    final_sql_string = ",\n".join(sql_lines_list)

    try:
        insert_query = text(f"""
        CREATE TABLE IF NOT EXISTS raw.bl_quarter (
        "Ticker" VARCHAR(10) NOT NULL,
        "year" INTEGER NOT NULL,
        "quarter" INTEGER NOT NULL,
        "data" JSONB,
        CONSTRAINT unique_bl_quarter UNIQUE ("Ticker", "year", "quarter" ));
        
        -- insert dữ liệu vào bảng jsonB để bắt đầu xử lý
        INSERT INTO raw.bl_quarter ("Ticker", "year", "quarter", "data")
        VALUES (:ticker, :year, :quarter, :data)
        
        ON CONFLICT ("Ticker", "year", "quarter") 
        DO UPDATE SET
            "data" = EXCLUDED."data";
        
        -- bắt đầu nạp dữ liệu vào bảng chính
        WITH Raw_Data_Exploded AS (
        -- Bước 1: Nổ (Flatten) JSONB đa cấp để tạo ra dữ liệu dạng DỌC (Long format)
        SELECT
            "Ticker",
            year, 
            quarter,
            (element ->> 'name') AS item_name,    -- Tên khoản mục (VD: Tổng doanh thu hoạt động)
            (elem_value ->> 'value'):: FLOAT:: BIGINT AS item_value, -- Giá trị tài chính tương ứng
            (element ->> 'id') as id
        FROM
            raw.bl_quarter,
            -- Nổ Lớp 1: Tách các khoản mục chính (A, B, C...)
            jsonb_array_elements(data) AS element,
            -- Nổ Lớp 2: Tách các quý (values) bên trong khoản mục
            jsonb_array_elements(element -> 'values') AS elem_value
        )
        INSERT INTO 
            raw.balance_sheet_quarter
        
        SELECT
            year, quarter,"Ticker", 
            {final_sql_string}
        FROM
            Raw_Data_Exploded
        GROUP BY
            "Ticker", year, quarter
        ON CONFLICT ("Ticker", year, quarter) DO NOTHING;
        
        DROP TABLE IF EXISTS raw.bl_quarter;
        
        
        
        """)  # Câu lệnh chèn dữ liệu với xử lý trùng lặp
        return insert_query
    except Exception as e:
        print(f"lỗi khi tạo câu lệnh SQL cho balance sheet")
        return None
 

def get_session():
    # Kiểm tra xem ông thợ này đã có đồ nghề (Session) trong túi chưa?
    thread_local = threading.local()
    if not hasattr(thread_local, "session"):
        # Nếu chưa -> Cấp mới 1 cái Session
        thread_local.session = requests.Session()
        auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
        
        # --- QUAN TRỌNG: CẢI TRANG ---
        # Đeo mặt nạ vào để Server tưởng mình là trình duyệt Chrome xịn, chứ không phải Python script
        thread_local.session.headers.update({
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://fireant.vn/",
            "authorization": auth_token}
        )
    return thread_local.session

    
def process_ticker_quarter(ticker):
    insert_query = _SQL_for_bl_quarter()
    s = get_session()
    try:
        start = 2020
        end = 2025
        years = range(end, start-1, -1)
        quarters = [4,3,2,1]
        with engine.begin() as conn: # Tự động commit/rollback
            for y in years: # tạo danh sách url tự động từ năm end đến start
                for q in quarters:
                    url = f'https://restv2.fireant.vn/symbols/{ticker}/full-financial-reports?type=1&year={y}&quarter={q}&limit=1' # URL API
                    
                    params = { 'type': '2', 
                    'year': str(y), 
                    'quarter': str(q), 
                    'limit': '1'}
                    
                    response = s.get(url, params=params, timeout = 10) # Gửi yêu cầu GET
                    if response.status_code == 200:
                        data = response.json() # đổi thành object trong python
                        if not data:
                            continue
                        resp_json = json.dumps(data, ensure_ascii=False) # Chuyển object thành chuỗi JSON
                        conn.execute(insert_query, {"ticker": ticker, "year": y, "quarter": q, "data": resp_json}) # Thực thi câu lệnh chèn dữ liệu
                    else: 
                        print(f"Failed to get data for {ticker} in {y}-{q}")
                        continue
                    time.sleep(0.2) # Thời gian chờ giữa các yêu cầu để tránh bị chặn 
            _db_cleanup(engine, 'balance_sheet_quarter')
                    
    except KeyboardInterrupt:
        print("lệnh dừng kích hoạt khi đang chạy dữ liệu quý ")
        raise          
    except Exception as e:
        print(f"Failed to process {ticker}: {e}")
        traceback.print_exc()
        raise e


# bộ cập nhật dữ liệu năm 
def process_ticker_year(ticker):
    try:
        with engine.begin() as conn: # Tự động commit/rollback
           # xử lý dữ liệu năm
            print(f"đang xử lý {ticker}")
            df_balancesheet = balance_sheet_year(ticker)
            ticker_lower = ticker.lower()
            temp_table_name = f"temp_table_{ticker_lower}"
            df_balancesheet.to_sql(
                name= temp_table_name,
                con=conn,
                schema='raw',   
                if_exists='replace', 
                index=False)

            # Gộp dữ liệu vào bảng chính
            sql_merge = text(f"""
                
                INSERT INTO raw.balance_sheet_year
                SELECT * FROM raw.{temp_table_name}
                WHERE year >= 2018
                ON CONFLICT ("Ticker", year, quarter) 
                DO NOTHING;  
                
                DROP TABLE IF EXISTS raw.{temp_table_name};
                
            """)
            
            conn.execute(sql_merge)
            _db_cleanup(engine, 'balance_sheet_year')
        time_sleep = random.uniform(0.2, 0.4)
        time.sleep(time_sleep)
        
    except KeyboardInterrupt:
        print(f"⚠️ Lệnh dừng kích hoạt khi đang xử lý {ticker}")
        raise # Ném lỗi lên cho Vòng lặp chính

    except Exception as e:
        print(f"❌ Lỗi {ticker}: {str(e)}")
        raise e
    finally:
        try:
            with engine.begin() as conn_cleanup:
                conn_cleanup.execute(text(f"DROP TABLE IF EXISTS raw.{temp_table_name};"))
        except:
            pass # Nếu không có bảng để xóa thì thôi, kệ nó


# hàm đọc danh sách ticker từ database
def _read_tickers_from_db(table):
    balance_tickers = set()
    df_tickers = pd.read_sql('SELECT "Ticker" FROM analysis_data.companies_list', engine)
    ticker_list = set(df_tickers['Ticker'].str.strip())
    if inspector.has_table(table, schema='raw'):
        df_all_balance = pd.read_sql(f'SELECT "Ticker" FROM raw.{table}', engine)
        balance_tickers = set(df_all_balance['Ticker'].str.strip())
    missing_tickers = set(ticker_list - balance_tickers)
    print(f"đang thiếu dữ liệu cho {len(missing_tickers)} cổ phiếu")
    return missing_tickers, ticker_list


# hàm chính chạy
def balance_sheet_update(engine, type):
    # nạp dữ liệu năm trước
    if type == 'year':
        missing_tickers_year, ticker_list_year = _read_tickers_from_db('balance_sheet_year')
        if len(missing_tickers_year) > 0:
            print (f"Có {len(missing_tickers_year)} ticker chưa xử lý")
            todos_year = missing_tickers_year
        else:
            print (f"Tất cả ticker đã xử lý, đang update dữ liệu")
            todos_year = ticker_list_year
        # nạp procedure SQL vào database postgresql
        _db_update_procedure(engine)
        if todos_year:
            try:
                for i, ticker in enumerate(todos_year):
                    try:
                        process_ticker_year(ticker)
                        print(f" xử lý xong ticker {i+1}/{len(todos_year)}: {ticker}")
                    except Exception as e:
                        print(f"❌ Lỗi khi xử lý ticker {ticker}: {e}")
                        continue
            except KeyboardInterrupt:
                print("lệnh dừng kích hoạt thành công ")
                sys.exit(0)
            finally:
                _db_cleanup(engine,'balance_sheet_year')
    # nạp dữ liệu quý
    if type == 'quarter':
        missing_tickers_quarter, ticker_list_quarter = _read_tickers_from_db('balance_sheet_quarter')
        if len(missing_tickers_quarter) > 0:
            print (f"Có {len(missing_tickers_quarter)} ticker chưa xử lý")
            todos_quarter = missing_tickers_quarter
        else:
            print (f"Tất cả ticker đã xử lý, đang update dữ liệu")
            todos_quarter = ticker_list_quarter
        if todos_quarter:
            try:
                with ThreadPoolExecutor(max_workers= 2) as executor:
                    future_to_ticker = {}
                    for ticker in todos_quarter:
                        future = executor.submit(process_ticker_quarter, ticker) # gửi từng ticker vào hàm xử lý, trả về future
                        future_to_ticker[future] = ticker
                    # theo dõi tiến trình bằng as_completed
                    try:
                        for i,future in enumerate(as_completed(future_to_ticker)):
                            ticker = future_to_ticker[future]
                            try:
                                future.result()
                                print(f" xử lý xong ticker {i+1}/{len(todos_quarter)}: {ticker}")
                            except Exception as e:
                                print(f"❌ Lỗi khi xử lý ticker {ticker}: {e}")
                                continue
                    except KeyboardInterrupt:
                        print("lệnh dừng kích hoạt thành công trong quá trình chờ các tiến trình con ")
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise
            except KeyboardInterrupt:
                print("lệnh dừng kích hoạt thành công ")
                sys.exit(0)
            finally:
                _db_cleanup(engine,'balance_sheet_quarter')