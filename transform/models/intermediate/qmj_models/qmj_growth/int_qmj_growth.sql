{{ config(
    materialized='table', 
    tags=['intermediate', 'qmj_growth']) }}
{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract valid TTM metrics and calculate Book Equity & Working Capital
WITH base_metrics AS (
    SELECT 
        *,
        (total_equity - minority_interest - preferred_stock) AS book_equity,
        (current_assets - current_liabilities - cash_and_equivalents + short_term_debt + income_taxes_payable) AS working_capital
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),

-- STEP 2: Calculate the 1-year change in Working Capital
calc_delta_wc AS (
    SELECT 
        *,
        (working_capital - LAG(working_capital, 4) OVER w_ticker) AS delta_working_capital
    FROM base_metrics
    WINDOW w_ticker AS (PARTITION BY ticker ORDER BY absolute_quarter)
),

-- STEP 3: Derive Cash Flow from Operations (CFO) if not directly available
calc_cfo AS (
    SELECT 
        *,
        (net_income_ttm + depreciation_ttm - delta_working_capital - capex_ttm) AS derived_cfo_ttm
    FROM calc_delta_wc
),
-- STEP 4: Prepare historical data points (Lags) for Growth calculations
prepare_history AS (
    SELECT 
        *,
        -- Continuity Check: Gap between current quarter and 16 quarters (4 years) ago
        (absolute_quarter - LAG(absolute_quarter, 16) OVER w_ticker) AS quarter_gap_16,

        -- 1 Year Ago (Current Hurdle)
        LAG(total_assets, 4) OVER w_ticker AS assets_t_minus_1_yr,
        LAG(book_equity, 4) OVER w_ticker AS equity_t_minus_1_yr,

        -- 3 Years Ago (Historical Surplus)
        LAG(gross_profit_ttm, 12) OVER w_ticker AS gp_past,
        LAG(net_income_parent_ttm, 12) OVER w_ticker AS ni_parent_past,
        LAG(net_income_ttm, 12) OVER w_ticker AS ni_past,
        LAG(derived_cfo_ttm, 12) OVER w_ticker AS derived_cfo_past,
        LAG(net_revenue_ttm, 12) OVER w_ticker AS rev_past,
        LAG(total_assets, 12) OVER w_ticker AS assets_past,
        LAG(book_equity, 12) OVER w_ticker AS equity_past,
        LAG(risk_free_rate, 12) OVER w_ticker AS rf_past,

        -- 4 Years Ago (Historical Hurdle)
        LAG(total_assets, 16) OVER w_ticker AS assets_past_minus_1_yr,
        LAG(book_equity, 16) OVER w_ticker AS equity_past_minus_1_yr

    FROM calc_cfo
    WINDOW w_ticker AS (PARTITION BY ticker ORDER BY absolute_quarter)
),
-- STEP 5: Calculate the 5 core Growth metrics
calc_growth_scores AS (
    SELECT
        *,
        -- 1. Delta Gross Profits over Assets (GPOA)
        (
            (gross_profit_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr)) 
            - (gp_past - ((rf_past / 100) * assets_past_minus_1_yr))
        ) / NULLIF(assets_past, 0) AS delta_gpoa,

        -- 2. Delta Return on Equity (ROE)
        (
            (net_income_parent_ttm - ((risk_free_rate / 100) * equity_t_minus_1_yr)) 
            - (ni_parent_past - ((rf_past / 100) * equity_past_minus_1_yr))
        ) / NULLIF(equity_past, 0) AS delta_roe,

        -- 3. Delta Cash Flow over Assets (CFOA)
        (
            (derived_cfo_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr)) 
            - (derived_cfo_past - ((rf_past / 100) * assets_past_minus_1_yr))
        ) / NULLIF(assets_past, 0) AS delta_cfoa,

        -- 4. Delta Return on Assets (ROA)
        (
            (net_income_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr)) 
            - (ni_past - ((rf_past / 100) * assets_past_minus_1_yr))
        ) / NULLIF(assets_past, 0) AS delta_roa,

        -- 5. Delta Gross Margin (GMAR)
        (gross_profit_ttm - gp_past) / NULLIF(rev_past, 0) AS delta_gmar

    FROM prepare_history
),
-- STEP 6: Apply Data Quality Rules specific to Growth factors
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Ensure full 16-quarter history without gaps
                CASE WHEN quarter_gap_16 IS NULL OR quarter_gap_16 != 16 
                    THEN 'Err: Broken history for Growth (Gap != 16)' ELSE NULL END,

                -- Check for null values in the final metrics
                CASE WHEN delta_gpoa IS NULL THEN 'delta_gpoa is null' ELSE NULL END,
                CASE WHEN delta_roe IS NULL THEN 'delta_roe is null' ELSE NULL END,
                CASE WHEN delta_roa IS NULL THEN 'delta_roa is null' ELSE NULL END,
                CASE WHEN delta_cfoa IS NULL THEN 'delta_cfoa is null' ELSE NULL END,
                CASE WHEN delta_gmar IS NULL THEN 'delta_gmar is null' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM calc_growth_scores
)

-- STEP 7: Final Selection , injecting audit columns, and resolving final status
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    delta_gpoa,
    delta_roe,
    delta_cfoa,
    delta_roa,
    delta_gmar,
    
    -- Resolve Final Status
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason
    
    -- Auto-generated audit columns
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_dq_rules
    