{{ config(materialized='table',
    tags=['intermediate', 'ttm_metrics']
) }}

WITH ttm_calc AS (
    SELECT 
        -- 1. GIỮ LẠI THÔNG TIN CƠ BẢN
        ticker,
        year,
        quarter,
        
        -- Tính Alias để show ra bảng kết quả cho dễ nhìn
        (year * 4 + quarter) AS absolute_quarter,

        -- CHỖ NÀY LÀ CÚ CHỐT: Bung thẳng công thức vào trong LAG và phép trừ
        ((year * 4 + quarter) - LAG((year * 4 + quarter), 3) OVER w_window) AS quarter_gap,


        -- 2. CỘNG DỒN TTM 4 QUÝ CHO INCOME STATEMENT (Dùng w_ttm)
        SUM(gross_revenue) OVER w_ttm AS gross_revenue_ttm,
        SUM(net_revenue) OVER w_ttm AS net_revenue_ttm,
        SUM(cogs) OVER w_ttm AS cogs_ttm,
        SUM(gross_profit) OVER w_ttm AS gross_profit_ttm,
        SUM(profit_before_tax) OVER w_ttm AS profit_before_tax_ttm,
        SUM(interest_expense) OVER w_ttm AS interest_expense_ttm,
        SUM(net_income) OVER w_ttm AS net_income_ttm,
        SUM(net_income_parent) OVER w_ttm AS net_income_parent_ttm,

        -- 3. CỘNG DỒN TTM 4 QUÝ CHO CASH FLOW (Dùng w_ttm)
        SUM(cfo) OVER w_ttm AS cfo_ttm,
        SUM(depreciation) OVER w_ttm AS depreciation_ttm,
        SUM(capex) OVER w_ttm AS capex_ttm,

        -- 4. BÊ NGUYÊN BALANCE SHEET SANG (Không cộng dồn vì là số dư cuối kỳ)
        total_assets,
        total_equity,
        current_assets,
        current_liabilities,
        cash_and_equivalents,
        income_taxes_payable,
        minority_interest,
        total_liabilities,
        short_term_debt,
        long_term_debt,
        market_cap,
        shares_outstanding,
        preferred_stock

        {% set audit_cols = get_audit_columns('intermediate') %}
        {% for col in audit_cols %}
        , {{ col.expr }} AS {{ col.alias }}
        {% endfor %}

    FROM {{ ref('int_financial_report_joined') }}
    WHERE status = 'qualified' 
    WINDOW 
        w_ttm AS (PARTITION BY ticker ORDER BY year, quarter ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),
        w_window AS (PARTITION BY ticker ORDER BY year, quarter)
),
rf_rate AS (
    SELECT 
        absolute_quarter,
        risk_free_rate
    FROM {{ ref('macro_risk_free_rate') }}
)

SELECT 
    t.*,
    r.risk_free_rate,
    -- 6. GÁN NHÃN SỐ PHẬN CHUẨN XÁC
    CASE 
        WHEN quarter_gap = 3 THEN 'valid_ttm'
        ELSE 'broken_ttm' 
    END AS ttm_status
FROM ttm_calc t
LEFT JOIN rf_rate r 
ON t.absolute_quarter = r.absolute_quarter
