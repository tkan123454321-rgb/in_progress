{{ config(materialized='table',
    tags=['intermediate', 'financial_report_joined']
) }}

WITH income_statement AS (
    -- Thêm cờ has_is để lát nữa bắt lỗi
    SELECT *, 1 AS has_is 
    FROM {{ ref('silver_ic_quarter') }}
    WHERE status = 'qualified'
),

balance_sheet AS (
    -- Thêm cờ has_bs để lát nữa bắt lỗi
    SELECT *, 1 AS has_bs 
    FROM {{ ref('silver_bl_quarter') }}
    WHERE status = 'qualified'
),

cash_flow AS (
    -- Thêm cờ has_cf để lát nữa bắt lỗi
    SELECT *, 1 AS has_cf 
    FROM {{ ref('silver_cf_quarter') }}
    WHERE status = 'qualified'
),

fundamental AS (
    SELECT *, 1 AS has_fund 
    FROM {{ ref('silver_fundamental_quarter') }}
    WHERE status = 'qualified'
),

joined_data AS (
    SELECT 
        -- 1. CHỈ GỌI TÊN KHÓA (Trino tự động Coalesce nhờ mệnh đề USING)
        ticker,
        year,
        quarter,
        
        -- Lấy report_date (Ưu tiên bảng có dữ liệu sớm/chính xác nhất)
        
        -- 2. NHẶT NGUYÊN LIỆU TỪ INCOME STATEMENT
        is_stmt.gross_revenue,
        is_stmt.net_revenue,
        is_stmt.cogs,
        is_stmt.gross_profit,
        is_stmt.interest_expense,
        is_stmt.profit_before_tax,
        is_stmt.net_income,
        is_stmt.net_income_parent,

        -- 3. NHẶT NGUYÊN LIỆU TỪ BALANCE SHEET
        bs.total_assets,
        bs.total_equity,
        bs.current_assets,
        bs.current_liabilities,
        bs.cash_and_equivalents,
        bs.income_taxes_payable,
        bs.minority_interest,
        bs.total_liabilities,
        bs.short_term_debt,
        bs.long_term_debt,
        bs.retained_earnings,

        -- 4. NHẶT NGUYÊN LIỆU TỪ CASH FLOW
        cf.cfo,
        cf.depreciation,
        cf.capex,

        fund.market_cap,
        fund.shares_outstanding,
        fund.preferred_stock,

        -- 5. BỘ LỌC TỬ THẦN (Sử dụng cờ đánh dấu thay cho cột ticker)
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN is_stmt.has_is IS NULL THEN 'missing_income_statement' ELSE NULL END,
                CASE WHEN bs.has_bs IS NULL THEN 'missing_balance_sheet' ELSE NULL END,
                CASE WHEN cf.has_cf IS NULL THEN 'missing_cash_flow' ELSE NULL END,
                CASE WHEN fund.has_fund IS NULL THEN 'missing_fundamental' ELSE NULL END
            ), 
        '') AS unqualified_reason

        -- 6. THÊM AUDIT COLUMNS TỰ ĐỘNG
        {% set audit_cols = get_audit_columns('intermediate') %}
        {% for col in audit_cols %}
        , {{ col.expr }} AS {{ col.alias }}
        {% endfor %}

    FROM income_statement is_stmt
    FULL OUTER JOIN balance_sheet bs 
        USING (ticker, year, quarter)
    FULL OUTER JOIN cash_flow cf 
        USING (ticker, year, quarter)
    FULL OUTER JOIN fundamental fund 
        USING (ticker, year, quarter)
)

SELECT 
    *,
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status
FROM joined_data