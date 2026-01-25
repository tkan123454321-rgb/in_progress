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
from ETL_modules.db_utils import db_cleanup, get_session





engine = get_db_engine()
inspector = inspect(engine)

# hàm lấy dữ liệu bảng cân đối kế toán (năm) từ fireant
def balance_sheet_year(ticker):
    try: 
        year_limit = 2018
        year_today = time.localtime().tm_year
        limit = year_today - year_limit 
        auth_token ='Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
        url = f'https://restv2.fireant.vn/symbols/{ticker}/full-financial-reports?type=1&year=2025&quarter=0&limit={limit}'
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


        params = {
            'type': '1',
            'year': str(year_today),
            'quarter': '0',
            'limit': str(limit)
        }

        headers = {
            'User-Agent': user_agent,
            'Authorization': auth_token
            }

        response = requests.get(url, headers=headers, params=params)
        response = response.json()

        data = pd.DataFrame(response)
        df_raw = data
        #explode cột values thành nhiều hàng
        df_raw = df_raw.explode('values')
        df_normalized = pd.json_normalize(df_raw['values'])


        #nối df_raw với df_normalized
        df_raw = pd.concat([df_raw['name'].reset_index(drop=True), df_normalized], axis=1)
        df_raw = df_raw[['name','year','value']]

        # đánh số 1 số tên
        danh_sach =  ['- Giá trị hao mòn lũy kế','- Nguyên giá']
        mask = df_raw['name'].isin(danh_sach)
        cumcount_series = df_raw.groupby('name').cumcount().astype(str)
        df_raw['name'] =np.where(mask, df_raw['name'] + '_' + cumcount_series, df_raw['name'])

        df_raw = df_raw.pivot_table(index='name', columns='year', values='value').reset_index()
        

        #sắp xếp cột nguyên giá, hao mòn luỹ kế theo đúng thứ tự
        df_raw[['sorted name','sorted number']] = df_raw['name'].str.split('_', expand=True,n=1)
        df_raw['sorted number'] = pd.to_numeric(df_raw['sorted number'], errors='coerce').fillna(-1).astype(int)
        df_raw = df_raw.sort_values(by=['sorted name','sorted number'], ascending=[True, True])
        df_raw = df_raw.drop(columns=['sorted name', 'sorted number'])

        # gộp các giá trị trùng vào thành 1 hàng
        parts = df_raw['name'].str.rsplit('_', n=1, expand=True)
        phần_chữ = parts[0]
        phần_số = pd.to_numeric(parts[1], errors='coerce')

        #tạo điều kiện để so sanh danh sách 
        mask_target = (phần_chữ.isin(danh_sach)) & (phần_số.notna())

        # nhóm mỗi hàng thành 1 nhóm
        # đếm số cột năm trong dataframe
        years_column = len(df_raw.columns) - 1  # trừ đi 1 để loại bỏ cột 'name'
        group_start = (phần_số // years_column).astype('Int64')
        df_raw['group'] = np.where(mask_target, phần_chữ + '_' + group_start.astype(str), df_raw['name'])
        #tạo danh sách cột năm
        years = [c for c in df_raw.columns if c not in ('name','group')]

        # bắt đầu ép


        def first_nonnull(s): # tạo hàm để chọn ra giá trị ko rỗng trong cột năm
            nz = s.dropna()
            return nz.iloc[0] if not nz.empty else pd.NA

        agg_dict = {}
        for year in years:
            agg_dict[year] = first_nonnull
        
        

        df_grouped = df_raw.groupby('group', sort=False).agg(agg_dict).reset_index()
        

        # giữ thứ tự xuất hiện ban đầu của nhóm
        thứ_tự = '''
        A. Tài sản lưu động và đầu tư ngắn hạn	
        I. Tiền và các khoản tương đương tiền	
        1. Tiền	
        2. Các khoản tương đương tiền	
        II. Các khoản đầu tư tài chính ngắn hạn	
        1. Chứng khoán kinh doanh	
        2. Dự phòng giảm giá chứng khoán kinh doanh	
        3. Đầu tư nắm giữ đến ngày đáo hạn	
        III. Các khoản phải thu ngắn hạn	
        1. Phải thu ngắn hạn của khách hàng	
        2. Trả trước cho người bán	
        3. Phải thu nội bộ ngắn hạn	
        4. Phải thu theo tiến độ hợp đồng xây dựng	
        5. Phải thu về cho vay ngắn hạn	
        6. Phải thu ngắn hạn khác	
        7. Dự phòng phải thu ngắn hạn khó đòi	
        IV. Tổng hàng tồn kho	
        1. Hàng tồn kho	
        2. Dự phòng giảm giá hàng tồn kho	
        V. Tài sản ngắn hạn khác	
        1. Chi phí trả trước ngắn hạn	
        2. Thuế giá trị gia tăng được khấu trừ	
        3. Thuế và các khoản phải thu Nhà nước	
        4. Giao dịch mua bán lại trái phiếu chính phủ	
        5. Tài sản ngắn hạn khác
        B. Tài sản cố định và đầu tư dài hạn	
        I. Các khoản phải thu dài hạn	
        1. Phải thu dài hạn của khách hàng	
        2. Vốn kinh doanh tại các đơn vị trực thuộc	
        3. Phải thu dài hạn nội bộ	
        4. Phải thu về cho vay dài hạn	                                   
        5. Phải thu dài hạn khác	
        6. Dự phòng phải thu dài hạn khó đòi	
        II. Tài sản cố định	
        1. Tài sản cố định hữu hình	
        - Nguyên giá_0	
        - Giá trị hao mòn lũy kế_0	
        2. Tài sản cố định thuê tài chính	
        - Nguyên giá_1	
        - Giá trị hao mòn lũy kế_1
        3. Tài sản cố định vô hình	
        - Nguyên giá_2
        - Giá trị hao mòn lũy kế_2
        III. Bất động sản đầu tư	
        - Nguyên giá_3	
        - Giá trị hao mòn lũy kế_3	
        IV. Tài sản dở dang dài hạn	
        1. Chi phí sản xuất, kinh doanh dở dang dài hạn
        2. chi phí xây dựng cơ bản dở dang	
        V. Các khoản đầu tư tài chính dài hạn	
        1. Đầu tư vào công ty con	
        2. Đầu tư vào công ty liên kết, liên doanh	
        3. Đầu tư khác vào công cụ vốn	
        4. Dự phòng giảm giá đầu tư tài chính dài hạn	
        5. Đầu tư nắm giữ đến ngày đáo hạn	
        VI. Tổng tài sản dài hạn khác	
        1. Chi phí trả trước dài hạn	
        2. Tài sản Thuế thu nhập hoãn lại	
        3. Tài sản dài hạn khác	
        VII. Lợi thế thương mại	
        TỔNG CỘNG TÀI SẢN	
        NGUỒN VỐN	
        A. Nợ phải trả	
        I. Nợ ngắn hạn	
        1. Vay và nợ thuê tài chính ngắn hạn	
        2. Vay và nợ dài hạn đến hạn phải trả	
        3. Phải trả người bán ngắn hạn	
        4. Người mua trả tiền trước	
        5. Thuế và các khoản phải nộp nhà nước	
        6. Phải trả người lao động	
        7. Chi phí phải trả ngắn hạn	
        8. Phải trả nội bộ ngắn hạn	
        9. Phải trả theo tiến độ kế hoạch hợp đồng xây dựng	
        10. Doanh thu chưa thực hiện ngắn hạn	
        11. Phải trả ngắn hạn khác	
        12. Dự phòng phải trả ngắn hạn	
        13. Quỹ khen thưởng phúc lợi	
        14. Quỹ bình ổn giá	
        15. Giao dịch mua bán lại trái phiếu chính phủ	
        II. Nợ dài hạn	
        1. Phải trả người bán dài hạn	
        2. Chi phí phải trả dài hạn	
        3. Phải trả nội bộ về vốn kinh doanh	
        4. Phải trả nội bộ dài hạn	
        5. Phải trả dài hạn khác	
        6. Vay và nợ thuê tài chính dài hạn	
        7. Trái phiếu chuyển đổi	
        8. Thuế thu nhập hoãn lại phải trả	
        9. Dự phòng trợ cấp mất việc làm	
        10. Dự phòng phải trả dài hạn	
        11. Doanh thu chưa thực hiện dài hạn	
        12. Quỹ phát triển khoa học và công nghệ	
        B. Nguồn vốn chủ sở hữu	
        I. Vốn chủ sở hữu	
        1. Vốn đầu tư của chủ sở hữu	
        2. Thặng dư vốn cổ phần	
        3. Quyền chọn chuyển đổi trái phiếu	
        4. Vốn khác của chủ sở hữu	
        5. Cổ phiếu quỹ	
        6. Chênh lệch đánh giá lại tài sản	
        7. Chênh lệch tỷ giá hối đoái	
        8. Quỹ đầu tư phát triển	
        9. Quỹ dự phòng tài chính	
        10. Quỹ khác thuộc vốn chủ sở hữu	
        11. Lợi nhuận sau thuế chưa phân phối	
        - LNST chưa phân phối lũy kế đến cuối kỳ trước	
        - LNST chưa phân phối kỳ này	
        12. Nguồn vốn đầu tư xây dựng cơ bản	
        13. Quỹ hỗ trợ sắp xếp doanh nghiệp	
        14. Lợi ích của cổ đông không kiểm soát	
        II. Nguồn kinh phí và quỹ khác	
        1. Nguồn kinh phí	
        2. Nguồn kinh phí đã hình thành tài sản cố định	
        3. Quỹ dự phòng trợ cấp mất việc làm	
        TỔNG CỘNG NGUỒN VỐN
        '''
        fireant_list_raw = thứ_tự.strip().splitlines()
        fireant_list_processed = []
        for item in fireant_list_raw:
            item_sach = item.strip()
            fireant_list_processed.append(item_sach)
        df_grouped =df_grouped.set_index('group').reindex(fireant_list_processed).reset_index()

        #đổi tên để nhận diện tên cổ phiếu đang xem
        df_balancesheet = df_grouped.rename(columns={'group': f'{ticker}, Đơn vị tính: đồng'})
        df_balancesheet = df_balancesheet.set_index(df_balancesheet.columns[0])
        df_balancesheet = df_balancesheet.T
        df_balancesheet.columns.name = None
        df_balancesheet.index.name = 'year'
        df_balancesheet = df_balancesheet.reset_index()
        df_balancesheet.insert(1, 'quarter', 5)
        df_balancesheet.insert(2, 'Ticker', ticker)
        df_balancesheet.drop(columns = ['NGUỒN VỐN','3. Quỹ dự phòng trợ cấp mất việc làm'], inplace = True, errors = 'ignore')
        df_balancesheet.columns = ['year', 'quarter', 'Ticker', "current_assets","cash_and_equivalents","cash","cash_equivalents","st_investments","st_trading_securities","st_trading_securities_provision","st_held_to_maturity","st_receivables","st_receivables_customers","st_prepayments_to_suppliers","st_receivables_internal","st_receivables_construction","st_receivables_loans","st_receivables_other","st_bad_debt_provision","inventory_net","inventory_gross","inventory_provision","other_current_assets","st_prepaid_expenses","deductible_vat","tax_receivables","st_govt_bonds_repo", "st_other_assets_items" ,"non_current_assets","lt_receivables","lt_receivables_customers","lt_capital_in_subordinates","lt_receivables_internal","lt_receivables_loans","lt_receivables_other","lt_bad_debt_provision","fixed_assets","tangible_fixed_assets","tangible_cost","tangible_accum_depr","finance_lease_assets","finance_lease_cost","finance_lease_accum_depr","intangible_assets","intangible_cost","intangible_accum_depr","investment_property","investment_property_cost","investment_property_accum_depr","lt_work_in_progress_assets","lt_work_in_progress_business","construction_in_progress","lt_investments","invest_in_subsidiaries","invest_in_associates","invest_in_other_entity","lt_invest_provision","lt_held_to_maturity","other_non_current_assets","lt_prepaid_expenses","deferred_tax_assets","lt_other_assets_items","goodwill","total_assets","liabilities","current_liabilities","st_borrowings","current_portion_lt_debt","st_payables_suppliers","st_advances_from_customers","taxes_payable","payables_employees","st_accrued_expenses","st_payables_internal","payables_construction_progress","st_unearned_revenue","st_payables_other","st_provisions","bonus_welfare_fund","price_stabilization_fund","st_govt_bonds_repo_liab","non_current_liabilities","lt_payables_suppliers","lt_accrued_expenses","lt_payables_internal_capital","lt_payables_internal","lt_payables_other","lt_borrowings","convertible_bonds","deferred_tax_liabilities","provision_severance_allowance","lt_provisions","lt_unearned_revenue","science_tech_fund","owners_equity_group","owners_equity","share_capital","share_premium","convertible_bond_option","other_capital","treasury_shares","asset_revaluation_reserve","fx_reserve","development_fund","financial_reserve_fund","other_funds_equity","retained_earnings","retained_earnings_accum","retained_earnings_current","capital_construction_fund","enterprise_arrangement_fund","minority_interest","other_funds_group","budget_sources","budget_sources_fixed_assets","total_resources"
                                    ]
        df_balancesheet = df_balancesheet.map(lambda x: x.item() if hasattr(x,'item') else x)
        non_numeric_cols = ["Ticker"]
        numeric_col = [col for col in df_balancesheet.columns if col not in non_numeric_cols]
        for col in numeric_col:
            df_balancesheet[col] = pd.to_numeric(df_balancesheet[col], errors='coerce')
        return df_balancesheet
    except Exception as e:
        print(f"Lỗi khi xử lý cổ phiếu {ticker}: {e}")
        traceback.print_exc() # In ra chi tiết lỗi nằm ở dòng nào
        return None
  
  
# tạo hàm đọc trực tiếp procedure SQL từ file và nạp vào database
        
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
    try:
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
        print("Bắt đầu dọn dẹp database... xin chờ chút nhé!")
        db_cleanup(engine)
        