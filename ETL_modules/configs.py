# Danh sách các bảng cần làm sạch và cột dùng để so sánh
table_cleanup_config = [
    {
        "table_name": "balance_sheet_year", 
        "compare_cols": ["total_assets", "owners_equity"]
    },
    {
        "table_name": "balance_sheet_quarter", 
        "compare_cols": ["total_assets", "owners_equity"]
    },
    {
        "table_name": "income_statement_quarter", 
        "compare_cols": ["revenue", "net_income"]
    },
    {
        "table_name": "income_statement_year", 
        "compare_cols": ["revenue", "net_income"]
    }
]