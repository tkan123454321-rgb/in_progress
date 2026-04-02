{% macro get_financial_reports_column(report_type) %}
    
    {% if report_type == 'income_statement' %}
        {{ return([
            {'id': 3, 'alias': 'net_revenue', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Doanh thu thuần'},
            {'id': 4, 'alias': 'cogs', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Giá vốn hàng bán'},

            {'id': 6, 'alias': 'financial_income', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Doanh thu hoạt động tài chính'},
            {'id': 7, 'alias': 'financial_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí tài chính'},
            {'id': 701, 'alias': 'interest_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Chi phí lãi vay'},
            
            {'id': 8, 'alias': 'affiliate_profit', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi nhuận công ty liên kết'},
            
            {'id': 9, 'alias': 'selling_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí bán hàng'},
            {'id': 10, 'alias': 'admin_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí quản lý doanh nghiệp'},
            
            {'id': 12, 'alias': 'other_income', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Thu nhập khác'},
            {'id': 13, 'alias': 'other_expense', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí khác'},
            
            {'id': 16, 'alias': 'current_tax', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí thuế TNDN hiện hành'},
            {'id': 17, 'alias': 'deferred_tax', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Chi phí thuế TNDN hoãn lại'},

            {'id': 19, 'alias': 'net_income', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Lợi nhuận sau thuế doanh nghiệp'},
            {'id': 20, 'alias': 'minority_interest', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'LNST cổ đông không kiểm soát'},
            {'id': 21, 'alias': 'net_income_parent', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'LNST công ty mẹ'}
        ]) }}
    {% elif report_type == 'balance_sheet' %}
        {{ return([
            {'id': 2, 'alias': 'total_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'name_vn': 'Tổng cộng tài sản'},
            {'id': 301, 'alias': 'total_liabilities', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Nợ phải trả'},
            {'id': 302, 'alias': 'total_equity', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': False, 'name_vn': 'Vốn chủ sở hữu'},

            {'id': 101, 'alias': 'current_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tài sản ngắn hạn'},
            {'id': 30101, 'alias': 'current_liabilities', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Nợ ngắn hạn'},

            {'id': 3020111, 'alias': 'retained_earnings', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Lợi nhuận sau thuế chưa phân phối'},

            {'id': 10202, 'alias': 'fixed_assets', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Tài sản cố định'},
            {'id': 10103, 'alias': 'short_term_receivables', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Phải thu ngắn hạn'},
            {'id': 3010206, 'alias': 'long_term_debt', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'name_vn': 'Vay nợ dài hạn'}
        ]) }}
    {% elif report_type == 'cash_flow' %}
        {{ return([
            {'id': 7, 'alias': 'cash_end_period', 'type': 'DECIMAL(20,4)', 'is_mandatory': True, 'must_be_positive': True, 'check_conflict': True, 'name_vn': 'Tiền và tương đương tiền cuối kỳ'},
            {'id': 201, 'alias': 'capex', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': True, 'name_vn': 'Tiền chi mua sắm TSCĐ'},
            {'id': 304, 'alias': 'debt_repayment', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': True, 'name_vn': 'Tiền chi trả nợ gốc vay'},

            {'id': 104, 'alias': 'cfo_indirect', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'CFO gián tiếp'},
            {'id': 10201, 'alias': 'depreciation_indirect', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'Khấu hao (Gián tiếp)'},
            {'id': 10301, 'alias': 'receivables_change_indirect', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'Biến động phải thu (Gián tiếp)'},
            {'id': 10302, 'alias': 'inventory_change_indirect', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'Biến động hàng tồn kho (Gián tiếp)'},


            {'id': 109, 'alias': 'cfo_direct', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'CFO trực tiếp'},
            {'id': 101, 'alias': 'cash_in_direct', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'Tiền thu từ bán hàng (Trực tiếp)'},
            {'id': 102, 'alias': 'cash_out_supplier_direct', 'type': 'DECIMAL(20,4)', 'is_mandatory': False, 'must_be_positive': False, 'check_conflict': False, 'name_vn': 'Tiền trả người bán (Trực tiếp)'}
        ]) }}
        
    {% else %}
        {{ exceptions.raise_compiler_error("Bác nhập sai loại báo cáo rồi: " ~ report_type) }}
    {% endif %}
    
{% endmacro %}