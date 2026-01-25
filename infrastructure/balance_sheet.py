from sqlalchemy import text
from infrastructure.common import *




engine = get_db_engine()

def balance_sheet_infrastructure(engine): # hàm tạo cơ sở hạ tầng database
    with engine.begin() as connection:
        
        
        # tạo schema nếu chưa có 
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analysis_data;"))
        table_name = ["balance_sheet_year", "balance_sheet_quarter"]
        # tạo bảng cho cân đối kế toán theo năm nếu chưa có
        for table in table_name:
            year_limit = 2018 if table == "balance_sheet_year" else 2020
            connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS raw.{table} (
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                "Ticker" VARCHAR(10) NOT NULL,
                "current_assets" BIGINT,
                
                -- I. Tiền và tương đương tiền
                "cash_and_equivalents" BIGINT,
                "cash" BIGINT,                              -- 1. Tiền
                "cash_equivalents" BIGINT,                  -- 2. Tương đương tiền
                
                -- II. Đầu tư tài chính ngắn hạn
                "st_investments" BIGINT,
                "st_trading_securities" BIGINT,             -- 1. Chứng khoán kinh doanh
                "st_trading_securities_provision" BIGINT,   -- 2. Dự phòng giảm giá
                "st_held_to_maturity" BIGINT,               -- 3. Đầu tư nắm giữ đến đáo hạn
                
                -- III. Phải thu ngắn hạn
                "st_receivables" BIGINT,
                "st_receivables_customers" BIGINT,          -- 1. Phải thu KH
                "st_prepayments_to_suppliers" BIGINT,       -- 2. Trả trước người bán
                "st_receivables_internal" BIGINT,           -- 3. Phải thu nội bộ
                "st_receivables_construction" BIGINT,       -- 4. Phải thu tiến độ XD
                "st_receivables_loans" BIGINT,              -- 5. Phải thu về cho vay
                "st_receivables_other" BIGINT,              -- 6. Phải thu khác
                "st_bad_debt_provision" BIGINT,             -- 7. Dự phòng khó đòi
                
                -- IV. Hàng tồn kho
                "inventory_net" BIGINT,
                "inventory_gross" BIGINT,                   -- 1. Hàng tồn kho
                "inventory_provision" BIGINT,               -- 2. Dự phòng giảm giá
                
                -- V. Tài sản ngắn hạn khác
                "other_current_assets" BIGINT,
                "st_prepaid_expenses" BIGINT,               -- 1. Chi phí trả trước NH
                "deductible_vat" BIGINT,                    -- 2. Thuế GTGT khấu trừ
                "tax_receivables" BIGINT,                   -- 3. Thuế phải thu NN
                "st_govt_bonds_repo" BIGINT,                -- 4. Giao dịch mua bán lại TPCP (Mới)
                "st_other_assets_items" BIGINT,             -- 5. Tài sản NH khác

                -- B. TÀI SẢN DÀI HẠN
                "non_current_assets" BIGINT,
                
                -- I. Phải thu dài hạn
                "lt_receivables" BIGINT,
                "lt_receivables_customers" BIGINT,          -- 1. Phải thu KH dài hạn
                "lt_capital_in_subordinates" BIGINT,        -- 2. Vốn kinh doanh ở đơn vị trực thuộc
                "lt_receivables_internal" BIGINT,           -- 3. Phải thu nội bộ DH
                "lt_receivables_loans" BIGINT,              -- 4. Phải thu về cho vay DH
                "lt_receivables_other" BIGINT,              -- 5. Phải thu DH khác
                "lt_bad_debt_provision" BIGINT,             -- 6. Dự phòng khó đòi DH
                
                -- II. Tài sản cố định
                "fixed_assets" BIGINT,
                "tangible_fixed_assets" BIGINT,             -- 1. TSCĐ Hữu hình
                "tangible_cost" BIGINT,                     -- - Nguyên giá
                "tangible_accum_depr" BIGINT,               -- - Hao mòn
                "finance_lease_assets" BIGINT,              -- 2. TSCĐ Thuê tài chính
                "finance_lease_cost" BIGINT,                -- - Nguyên giá
                "finance_lease_accum_depr" BIGINT,          -- - Hao mòn
                "intangible_assets" BIGINT,                 -- 3. TSCĐ Vô hình
                "intangible_cost" BIGINT,                   -- - Nguyên giá
                "intangible_accum_depr" BIGINT,             -- - Hao mòn
                
                -- III. BĐS Đầu tư
                "investment_property" BIGINT,
                "investment_property_cost" BIGINT,          -- - Nguyên giá
                "investment_property_accum_depr" BIGINT,    -- - Hao mòn
                
                -- IV. Tài sản dở dang dài hạn
                "lt_work_in_progress_assets" BIGINT,
                "lt_work_in_progress_business" BIGINT,      -- 1. SXKD dở dang
                "construction_in_progress" BIGINT,          -- 2. XDCB dở dang
                
                -- V. Đầu tư tài chính dài hạn
                "lt_investments" BIGINT,
                "invest_in_subsidiaries" BIGINT,            -- 1. Công ty con
                "invest_in_associates" BIGINT,              -- 2. Liên doanh, liên kết
                "invest_in_other_entity" BIGINT,            -- 3. Đầu tư khác
                "lt_invest_provision" BIGINT,               -- 4. Dự phòng giảm giá
                "lt_held_to_maturity" BIGINT,               -- 5. Nắm giữ đến đáo hạn
                
                -- VI. Tài sản dài hạn khác
                "other_non_current_assets" BIGINT,
                "lt_prepaid_expenses" BIGINT,               -- 1. Chi phí trả trước DH
                "deferred_tax_assets" BIGINT,               -- 2. Thuế hoãn lại
                "lt_other_assets_items" BIGINT,             -- 3. Tài sản DH khác
                
                -- VII. Lợi thế thương mại
                "goodwill" BIGINT,
                
                -- TỔNG CỘNG TÀI SẢN
                "total_assets" BIGINT,

                -- ==================================================
                -- NGUỒN VỐN
                -- A. Nợ phải trả
                "liabilities" BIGINT,
                
                -- I. Nợ ngắn hạn
                "current_liabilities" BIGINT,
                "st_borrowings" BIGINT,                     -- 1. Vay & nợ thuê TC ngắn hạn
                "current_portion_lt_debt" BIGINT,           -- 2. Vay DH đến hạn trả
                "st_payables_suppliers" BIGINT,             -- 3. Phải trả người bán
                "st_advances_from_customers" BIGINT,        -- 4. Người mua trả tiền trước
                "taxes_payable" BIGINT,                     -- 5. Thuế phải nộp NN
                "payables_employees" BIGINT,                -- 6. Phải trả NLĐ
                "st_accrued_expenses" BIGINT,               -- 7. Chi phí phải trả
                "st_payables_internal" BIGINT,              -- 8. Phải trả nội bộ
                "payables_construction_progress" BIGINT,    -- 9. Phải trả theo tiến độ XD
                "st_unearned_revenue" BIGINT,               -- 10. Doanh thu chưa thực hiện
                "st_payables_other" BIGINT,                 -- 11. Phải trả khác
                "st_provisions" BIGINT,                     -- 12. Dự phòng phải trả
                "bonus_welfare_fund" BIGINT,                -- 13. Quỹ khen thưởng phúc lợi
                "price_stabilization_fund" BIGINT,          -- 14. Quỹ bình ổn giá (Mới)
                "st_govt_bonds_repo_liab" BIGINT,           -- 15. Giao dịch mua bán lại TPCP (Mới)
                
                -- II. Nợ dài hạn
                "non_current_liabilities" BIGINT,
                "lt_payables_suppliers" BIGINT,             -- 1. Phải trả người bán DH
                "lt_accrued_expenses" BIGINT,               -- 2. Chi phí phải trả DH
                "lt_payables_internal_capital" BIGINT,      -- 3. Phải trả nội bộ vốn KD
                "lt_payables_internal" BIGINT,              -- 4. Phải trả nội bộ DH
                "lt_payables_other" BIGINT,                 -- 5. Phải trả DH khác
                "lt_borrowings" BIGINT,                     -- 6. Vay & nợ thuê TC dài hạn
                "convertible_bonds" BIGINT,                 -- 7. Trái phiếu chuyển đổi
                "deferred_tax_liabilities" BIGINT,          -- 8. Thuế hoãn lại phải trả
                "provision_severance_allowance" BIGINT,     -- 9. Dự phòng mất việc làm
                "lt_provisions" BIGINT,                     -- 10. Dự phòng phải trả DH
                "lt_unearned_revenue" BIGINT,               -- 11. Doanh thu chưa thực hiện DH
                "science_tech_fund" BIGINT,                 -- 12. Quỹ KHCN

                -- B. Vốn chủ sở hữu
                "owners_equity_group" BIGINT,
                
                -- I. Vốn chủ sở hữu
                "owners_equity" BIGINT,
                "share_capital" BIGINT,                     -- 1. Vốn đầu tư của CSH
                "share_premium" BIGINT,                     -- 2. Thặng dư vốn cổ phần
                "convertible_bond_option" BIGINT,           -- 3. Quyền chọn TP chuyển đổi
                "other_capital" BIGINT,                     -- 4. Vốn khác
                "treasury_shares" BIGINT,                   -- 5. Cổ phiếu quỹ
                "asset_revaluation_reserve" BIGINT,         -- 6. Đánh giá lại tài sản
                "fx_reserve" BIGINT,                        -- 7. Chênh lệch tỷ giá
                "development_fund" BIGINT,                  -- 8. Quỹ đầu tư phát triển
                "financial_reserve_fund" BIGINT,            -- 9. Quỹ dự phòng tài chính
                "other_funds_equity" BIGINT,                -- 10. Quỹ khác
                "retained_earnings" BIGINT,                 -- 11. LNST chưa phân phối
                "retained_earnings_accum" BIGINT,           -- - Lũy kế
                "retained_earnings_current" BIGINT,         -- - Kỳ này
                "capital_construction_fund" BIGINT,         -- 12. Nguồn vốn XDCB
                "enterprise_arrangement_fund" BIGINT,       -- 13. Quỹ hỗ trợ sắp xếp DN (Mới)
                "minority_interest" BIGINT,                 -- 14. Lợi ích cổ đông không kiểm soát
                
                -- II. Nguồn kinh phí và quỹ khác
                "other_funds_group" BIGINT,
                "budget_sources" BIGINT,                    -- 1. Nguồn kinh phí
                "budget_sources_fixed_assets" BIGINT,       -- 2. Kinh phí đã hình thành TSCĐ
                "total_resources" BIGINT,
                
                CONSTRAINT PK_{table} PRIMARY KEY ("Ticker", year, quarter),
                CONSTRAINT fk_{table} FOREIGN KEY ("Ticker")
                    REFERENCES analysis_data.companies_list("Ticker") 
                        ON DELETE 
                            CASCADE,
                CONSTRAINT chk_{table} CHECK (year >= {year_limit} AND year <= EXTRACT(YEAR FROM CURRENT_DATE))
                    );    
                """))
            
