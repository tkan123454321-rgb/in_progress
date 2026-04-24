{{ config(
    materialized='table', 
    tags=['intermediate', 'z_qmj_safety']) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract valid TTM metrics and calculate EBIT & Working Capital
WITH base_metrics AS (
    SELECT 
        *,
        -- EBIT = Profit Before Tax + Interest Expense
        (profit_before_tax_ttm + interest_expense_ttm) AS ebit_ttm,
        
        -- Working Capital = Current Assets - Current Liabilities
        (current_assets - current_liabilities) AS working_capital
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),

-- STEP 2: Calculate the 5 core ratios of the Altman Z-Score model (X1 to X5)
calc_z_components AS (
    SELECT
        *,
        -- Standardize by Total Assets (AT)
        (working_capital / NULLIF(total_assets, 0)) AS x1_wc_at,
        (retained_earnings / NULLIF(total_assets, 0)) AS x2_re_at,
        (ebit_ttm / NULLIF(total_assets, 0)) AS x3_ebit_at,
        (market_cap / NULLIF(total_assets, 0)) AS x4_me_at,
        (net_revenue_ttm / NULLIF(total_assets, 0)) AS x5_sale_at
    FROM base_metrics
),
-- STEP 3: Apply the standard Altman Z-Score weighting formula
calc_z_score AS (
    SELECT
        *,
        (1.2 * x1_wc_at + 1.4 * x2_re_at + 3.3 * x3_ebit_at + 0.6 * x4_me_at + 1.0 * x5_sale_at) AS altman_z_score
    FROM calc_z_components
),

-- STEP 4: Apply Data Quality Rules specific to Altman Z-Score
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Altman model uses point-in-time data, no lags required. Just check components.
                CASE WHEN working_capital IS NULL THEN 'wc is null' ELSE NULL END,
                CASE WHEN retained_earnings IS NULL THEN 're is null' ELSE NULL END,
                CASE WHEN ebit_ttm IS NULL THEN 'ebit is null' ELSE NULL END,
                CASE WHEN market_cap IS NULL THEN 'market_cap is null' ELSE NULL END,
                CASE WHEN net_revenue_ttm IS NULL THEN 'revenue is null' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM calc_z_score
)

-- STEP 5: Final Selection and Status Resolution
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
    
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified' 
        ELSE 'unqualified' 
    END AS status,
    
    unqualified_reason
    
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_dq_rules