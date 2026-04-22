{{ config(materialized='table', tags=['intermediate', 'qmj', 'safety']) }}

WITH base_metrics AS (
    SELECT 
        *,
        -- 1. EBIT = Profit Before Tax + Interest Expense
        (profit_before_tax_ttm + interest_expense_ttm) AS ebit_ttm,
        
        -- 2. Working Capital = Current Assets - Current Liabilities
        (current_assets - current_liabilities) AS working_capital
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),
calc_z_components AS (
    SELECT
        *,
        -- Chia tất cả cho Total Assets (AT) để ra các tỷ số X1 -> X5
        (working_capital / NULLIF(total_assets, 0)) AS x1_wc_at,
        (retained_earnings / NULLIF(total_assets, 0)) AS x2_re_at,
        (ebit_ttm / NULLIF(total_assets, 0)) AS x3_ebit_at,
        (market_cap / NULLIF(total_assets, 0)) AS x4_me_at,
        (net_revenue_ttm / NULLIF(total_assets, 0)) AS x5_sale_at
    FROM base_metrics
),
calc_z_score AS (
    SELECT
        *,
        -- Áp dụng trọng số chuẩn của Altman Z-Score
        (1.2 * x1_wc_at + 1.4 * x2_re_at + 3.3 * x3_ebit_at + 0.6 * x4_me_at + 1.0 * x5_sale_at) AS altman_z_score
    FROM calc_z_components
),
apply_dq AS (
    SELECT 
        *,
        -- Sử dụng nhánh 'z_score_safety' trong macro
        {{ check_qmj_column('z_score_safety') }} AS unqualified_reason
    FROM calc_z_score
)

SELECT 
    ticker, 
    year, 
    quarter, 
    absolute_quarter,
    x1_wc_at, 
    x2_re_at, 
    x3_ebit_at, 
    x4_me_at, 
    x5_sale_at,
    altman_z_score,
    CASE WHEN unqualified_reason IS NULL THEN 'qualified' ELSE 'unqualified' END AS status,
    unqualified_reason
    
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
FROM apply_dq