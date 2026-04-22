{{ config(materialized='table', tags=['intermediate', 'qmj', 'growth']) }}

WITH calc_wc AS (
    SELECT 
        *,
        (total_equity - minority_interest - preferred_stock) AS book_equity,
        (current_assets - current_liabilities - cash_and_equivalents + short_term_debt + income_taxes_payable) AS working_capital
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),
calc_delta_wc AS (
    SELECT 
        *,
        (working_capital - LAG(working_capital, 4) OVER w) AS delta_working_capital
    FROM calc_wc
    WINDOW w AS (PARTITION BY ticker ORDER BY absolute_quarter)
),
calc_cfo AS (
    SELECT 
        *,
        (net_income_ttm + depreciation_ttm - delta_working_capital - capex_ttm) AS derived_cfo_ttm
    FROM calc_delta_wc
),
prepare_history AS (
    SELECT 
        *,
        -- 🛑 TÍNH QUARTER GAP ĐỂ CHECK ĐỘ LIỀN MẠCH (Lùi 16 quý = 4 năm)
        (absolute_quarter - LAG(absolute_quarter, 16) OVER w) AS quarter_gap_16,

        -- Vốn đầu năm nay (Hurdle hiện tại)
        LAG(total_assets, 4) OVER w AS assets_t_minus_1_yr,
        LAG(book_equity, 4) OVER w AS equity_t_minus_1_yr,

        -- Dữ liệu 3 năm trước (Thặng dư quá khứ)
        LAG(gross_profit_ttm, 12) OVER w AS gp_past,
        LAG(net_income_parent_ttm, 12) OVER w AS ni_parent_past,
        LAG(net_income_ttm, 12) OVER w AS ni_past,
        LAG(derived_cfo_ttm, 12) OVER w AS derived_cfo_past,
        LAG(net_revenue_ttm, 12) OVER w AS rev_past,
        LAG(total_assets, 12) OVER w AS assets_past,
        LAG(book_equity, 12) OVER w AS equity_past,
        LAG(risk_free_rate, 12) OVER w AS rf_past,

        -- Vốn của 4 năm trước (Hurdle quá khứ)
        LAG(total_assets, 16) OVER w AS assets_past_minus_1_yr,
        LAG(book_equity, 16) OVER w AS equity_past_minus_1_yr
    FROM calc_cfo
    WINDOW w AS (PARTITION BY ticker ORDER BY absolute_quarter)
),
calc_growth_scores AS (
    SELECT
        *,
        (
            (gross_profit_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr)) 
            - 
            (gp_past - ((rf_past / 100) * assets_past_minus_1_yr))
        ) / NULLIF(assets_past, 0) AS delta_gpoa,

        (
            (net_income_parent_ttm - ((risk_free_rate / 100) * equity_t_minus_1_yr)) 
            - 
            (ni_parent_past - ((rf_past / 100) * equity_past_minus_1_yr))
        ) / NULLIF(equity_past, 0) AS delta_roe,

        (
            (derived_cfo_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr)) 
            - 
            (derived_cfo_past - ((rf_past / 100) * assets_past_minus_1_yr))
        ) / NULLIF(assets_past, 0) AS delta_cfoa,

        (
            (net_income_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr)) 
            - 
            (ni_past - ((rf_past / 100) * assets_past_minus_1_yr))
        ) / NULLIF(assets_past, 0) AS delta_roa,

        (gross_profit_ttm - gp_past) / NULLIF(rev_past, 0) AS delta_gmar
    FROM prepare_history
),
apply_dq AS (
    SELECT 
        *,
        {{ check_qmj_column('growth') }} AS unqualified_reason
    FROM calc_growth_scores
)

select 
    ticker,
    year,
    quarter,
    absolute_quarter,
    delta_gpoa,
    delta_roe,
    delta_cfoa,
    delta_roa,
    delta_gmar,
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason
    
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
from apply_dq
    