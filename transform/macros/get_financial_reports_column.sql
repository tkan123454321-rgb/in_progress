{% macro get_financial_reports_column(report_type) %}
    
    {% if report_type == 'income_statement' %}
        {{ return([
            {'id': 1, 'alias': 'gross_revenue', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tổng doanh thu hoạt động kinh doanh'},
            {'id': 2, 'alias': 'revenue_deduction', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Các khoản giảm trừ doanh thu'},
            {'id': 3, 'alias': 'net_revenue', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Doanh thu thuần'},
            {'id': 4, 'alias': 'cogs', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Giá vốn hàng bán'},
            {'id': 5, 'alias': 'gross_profit', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Lợi nhuận gộp'},

            {'id': 6, 'alias': 'financial_income', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Doanh thu hoạt động tài chính'},
            {'id': 7, 'alias': 'financial_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí tài chính'},
            {'id': 701, 'alias': 'interest_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí lãi vay'},
            
            {'id': 8, 'alias': 'affiliate_profit', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi nhuận công ty liên kết'},
            
            {'id': 9, 'alias': 'selling_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí bán hàng'},
            {'id': 10, 'alias': 'admin_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí quản lý doanh nghiệp'},
            
            {'id': 11, 'alias': 'operating_profit', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Lợi nhuận thuần từ HĐKD'},

            {'id': 12, 'alias': 'other_income', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Thu nhập khác'},
            {'id': 13, 'alias': 'other_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí khác'},
            {'id': 14, 'alias': 'other_profit', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi nhuận khác'},

            {'id': 15, 'alias': 'profit_before_tax', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Tổng lợi nhuận trước thuế'},

            {'id': 16, 'alias': 'current_tax', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí thuế TNDN hiện hành'},
            {'id': 17, 'alias': 'deferred_tax', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí thuế TNDN hoãn lại'},
            {'id': 18, 'alias': 'income_tax_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tổng chi phí thuế TNDN'},

            {'id': 19, 'alias': 'net_income', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Lợi nhuận sau thuế doanh nghiệp'},
            {'id': 20, 'alias': 'minority_interest', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'LNST cổ đông không kiểm soát'},
            {'id': 21, 'alias': 'net_income_parent', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'LNST công ty mẹ'}
        ]) }}
    {% elif report_type == 'balance_sheet' %}
        {{ return([
            {'id': 2, 'alias': 'total_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Tổng cộng tài sản'},
            {'id': 4, 'alias': 'total_capital', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Tổng cộng nguồn vốn'},

            {'id': 101, 'alias': 'current_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tài sản ngắn hạn'},
            {'id': 102, 'alias': 'long_term_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tài sản dài hạn'},

            {'id': 301, 'alias': 'total_liabilities', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Nợ phải trả'},
            
            {'id': 302, 'alias': 'total_equity', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Vốn chủ sở hữu'},

            {'id': 30101, 'alias': 'current_liabilities', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Nợ ngắn hạn'},
            {'id': 30102, 'alias': 'long_term_liabilities', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Nợ dài hạn'},

            {'id': 10101, 'alias': 'cash_and_equivalents', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tiền và tương đương tiền'},
            {'id': 3010105, 'alias': 'income_taxes_payable', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Thuế và các khoản phải nộp nhà nước'},
            
            
            {'id': 3020111, 'alias': 'retained_earnings', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi nhuận sau thuế chưa phân phối'},
            {'id': 3020114, 'alias': 'minority_interest', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi ích của cổ đông không kiểm soát'},
            {'id': 10202, 'alias': 'fixed_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tài sản cố định'},
            {'id': 10103, 'alias': 'short_term_receivables', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Phải thu ngắn hạn'},

            {'id': 3010101, 'alias': 'short_term_debt', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Vay và nợ thuê tài chính ngắn hạn'},
            {'id': 3010102, 'alias': 'current_portion_lt_debt', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Vay và nợ dài hạn đến hạn phải trả'},
            {'id': 3010115, 'alias': 'repo_transactions', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Giao dịch mua bán lại trái phiếu CP'},
            {'id': 3010206, 'alias': 'long_term_debt', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Vay nợ dài hạn'},
            {'id': 3010207, 'alias': 'convertible_bonds', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Trái phiếu chuyển đổi'}
        ]) }}
    {% elif report_type == 'cash_flow_indirect' %}
        {{ return([
            {'id': 104, 'alias': 'cfo', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Lưu chuyển tiền thuần từ hoạt động kinh doanh'},
            {'id': 212, 'alias': 'cfi', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lưu chuyển tiền thuần từ hoạt động đầu tư'},
            {'id': 311, 'alias': 'cff', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lưu chuyển tiền thuần từ hoạt động tài chính'},
            {'id': 4,   'alias': 'net_cash_flow', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Lưu chuyển tiền thuần trong kỳ'},

            {'id': 10201, 'alias': 'depreciation', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Khấu hao TSCĐ'},
            {'id': 201, 'alias': 'capex', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác'},
            
            {'id': 101, 'alias': 'profit_before_tax_cf', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi nhuận trước thuế'},

            {'id': 10301, 'alias': 'delta_receivables', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tăng, giảm các khoản phải thu'},
            {'id': 10302, 'alias': 'delta_inventory', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tăng, giảm hàng tồn kho'},
            {'id': 10303, 'alias': 'delta_payables', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tăng, giảm các khoản phải trả'},

            {'id': 303, 'alias': 'borrowings_received', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tiền vay ngắn hạn, dài hạn nhận được'},
            {'id': 304, 'alias': 'debt_repaid', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tiền chi trả nợ gốc vay'},
            {'id': 308, 'alias': 'dividends_paid', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Cổ tức, lợi nhuận đã trả cho chủ sở hữu'},

            {'id': 5, 'alias': 'beginning_cash', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Tiền và tương đương tiền đầu kỳ'},
            {'id': 6, 'alias': 'exchange_rate_effect', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Ảnh hưởng của thay đổi tỷ giá'},
            {'id': 7, 'alias': 'ending_cash', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Tiền và tương đương tiền cuối kỳ'}
        ]) }}
    {% elif report_type == 'fundamental_quarter' %}
        {{ return([
            {'json_key': 'PreferredStock', 'alias': 'preferred_stock', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Cổ phiếu ưu đãi'},
            {'json_key': 'MarketCapAtPeriodEnd', 'alias': 'market_cap', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Vốn hóa thị trường cuối kỳ'},
            {'json_key': 'ShareAtPeriodEnd', 'alias': 'shares_outstanding', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Số lượng cổ phiếu lưu hành'}
        ]) }}
    {% elif report_type == 'historical_quotes' %}
        {{ return([
            {'json_key': 'priceBasic', 'alias': 'price_basic', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Giá tham chiếu'},
            {'json_key': 'priceOpen', 'alias': 'price_open', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Giá mở cửa'},
            {'json_key': 'priceHigh', 'alias': 'price_high', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Giá cao nhất'},
            {'json_key': 'priceLow', 'alias': 'price_low', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Giá thấp nhất'},
            {'json_key': 'priceClose', 'alias': 'price_close', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Giá đóng cửa'},
            {'json_key': 'totalVolume', 'alias': 'total_volume', 'type': 'DOUBLE', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tổng khối lượng'},
            {'json_key': 'totalValue', 'alias': 'total_value', 'type': 'DOUBLE', 'is_mandatory': False, 'must_be_positive': True, 'name_vn': 'Tổng giá trị'}
        ]) }}
        
    {% else %}
        {{ exceptions.raise_compiler_error("Bác nhập sai loại báo cáo rồi: " ~ report_type) }}
    {% endif %}
    
{% endmacro %}