{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_profitability']
) }}

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

-- STEP 2: Calculate 1-year change in Working Capital and check continuity
calc_delta_wc AS (
    SELECT 
        *,
        (working_capital - LAG(working_capital, 4) OVER w_ticker) AS delta_working_capital,
        (absolute_quarter - LAG(absolute_quarter, 4) OVER w_ticker) AS quarter_gap_wc
    FROM base_metrics
    WINDOW w_ticker AS (PARTITION BY ticker ORDER BY absolute_quarter)
),

-- STEP 3: Calculate the 6 core Profitability metrics
calc_profitability_scores AS (
    SELECT 
        *,
        (gross_profit_ttm / NULLIF(total_assets, 0)) AS gpoa,
        (net_income_parent_ttm / NULLIF(book_equity, 0)) AS roe,
        (net_income_ttm / NULLIF(total_assets, 0)) AS roa,
        (gross_profit_ttm / NULLIF(net_revenue_ttm, 0)) AS gmar,
        ((net_income_ttm + depreciation_ttm - delta_working_capital - capex_ttm) / NULLIF(total_assets, 0)) AS cfoa,
        ((depreciation_ttm - delta_working_capital) / NULLIF(total_assets, 0)) AS acc
    FROM calc_delta_wc
),

-- STEP 4: Apply Data Quality Rules specific to Profitability factors
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Continuity Check: Must have a 4-quarter gap for Delta WC
                CASE WHEN quarter_gap_wc IS NULL OR quarter_gap_wc != 4 
                    THEN 'Err: Missing historical quarters for Delta WC (Gap != 4)' ELSE NULL END,

                -- Null Checks for the 6 core components
                CASE WHEN gpoa IS NULL THEN 'gpoa is null' ELSE NULL END,
                CASE WHEN roe IS NULL THEN 'roe is null' ELSE NULL END,
                CASE WHEN roa IS NULL THEN 'roa is null' ELSE NULL END,
                CASE WHEN gmar IS NULL THEN 'gmar is null' ELSE NULL END,
                CASE WHEN cfoa IS NULL THEN 'cfoa is null' ELSE NULL END,
                CASE WHEN acc IS NULL THEN 'acc is null' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM calc_profitability_scores
)

-- STEP 5: Final Selection and Status Resolution
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    gpoa,
    roe,
    roa,
    gmar,
    cfoa,
    acc,
    
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