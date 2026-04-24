{{ config(
    materialized='table', 
    tags=['intermediate', 'o_qmj_safety']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract valid TTM metrics and calculate base financial components
WITH base_metrics AS (
    SELECT 
        *,
        -- Book Equity
        (total_equity - minority_interest - preferred_stock) AS book_equity,
        
        -- Adjusted Assets: AT + 0.1 * (Market Equity - Book Equity)
        (total_assets + 0.1 * (market_cap - (total_equity - minority_interest - preferred_stock))) AS adj_asset,
        
        -- Total Debt: Short Term + Long Term
        (short_term_debt + long_term_debt) AS total_debt

    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),

-- STEP 2: Prepare lagged metrics for Year-over-Year comparison
prep_lags AS (
    SELECT 
        *,
        -- Retrieve Net Income from 1 year ago (4 quarters prior)
        LAG(net_income_ttm, 4) OVER w_window AS net_income_past,
        
        -- Check data continuity (must exactly equal 4 for a 1-year gap)
        (absolute_quarter - LAG(absolute_quarter, 4) OVER w_window) AS quarter_gap_4
        
    FROM base_metrics
    WINDOW w_window AS (PARTITION BY ticker ORDER BY absolute_quarter)
),

-- STEP 3: Calculate the 9 core variables of the Ohlson O-Score model
calc_ohlson_vars AS (
    SELECT
        *,
        -- Used the pre-calculated adj_asset to simplify log_size
        LN(NULLIF(adj_asset, 0) / NULLIF(cpi_index, 0)) AS log_size,
        (total_debt / NULLIF(adj_asset, 0)) AS tlta,
        ((current_assets - current_liabilities) / NULLIF(adj_asset, 0)) AS wcta,
        (current_liabilities / NULLIF(current_assets, 0)) AS clca,
        CASE WHEN total_liabilities > total_assets THEN 1 ELSE 0 END AS oeneg,
        (net_income_ttm / NULLIF(total_assets, 0)) AS nita,
        (profit_before_tax_ttm / NULLIF(total_liabilities, 0)) AS futl,
        CASE WHEN GREATEST(net_income_ttm, net_income_past) < 0 THEN 1 ELSE 0 END AS intwo,
        (net_income_ttm - net_income_past) / NULLIF(ABS(net_income_ttm) + ABS(net_income_past), 0) AS chin
    FROM prep_lags
),

-- STEP 4: Calculate the final Ohlson O-Score
calc_o_score AS (
    SELECT
        *,
        -- The final linear equation for default probability
        -(
            -1.32 
            - 0.407 * log_size 
            + 6.03 * tlta
            - 1.43 * wcta
            + 0.076 * clca
            - 1.72 * oeneg
            - 2.37 * nita
            - 1.83 * futl
            + 0.285 * intwo
            - 0.521 * chin
        ) AS ohlson_o_score
    FROM calc_ohlson_vars
),

-- STEP 5: Apply Data Quality Rules specific to Ohlson O-Score
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Continuity Check
                CASE WHEN quarter_gap_4 IS NULL OR quarter_gap_4 != 4 
                    THEN 'Err: Broken history for O-Score (Gap != 4)' ELSE NULL END,
                
                -- Null Checks for all 9 components
                CASE WHEN log_size IS NULL THEN 'log_size is null (Check Assets/CPI)' ELSE NULL END,
                CASE WHEN tlta IS NULL THEN 'tlta is null' ELSE NULL END,
                CASE WHEN wcta IS NULL THEN 'wcta is null' ELSE NULL END,
                CASE WHEN clca IS NULL THEN 'clca is null' ELSE NULL END,
                CASE WHEN oeneg IS NULL THEN 'oeneg is null' ELSE NULL END,
                CASE WHEN nita IS NULL THEN 'nita is null' ELSE NULL END,
                CASE WHEN futl IS NULL THEN 'futl is null' ELSE NULL END,
                CASE WHEN intwo IS NULL THEN 'intwo is null' ELSE NULL END,
                CASE WHEN chin IS NULL THEN 'chin is null' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM calc_o_score
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    log_size,
    tlta,
    wcta,
    clca,
    oeneg,
    nita,
    futl,
    intwo,
    chin,
    ohlson_o_score,
    
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,

    unqualified_reason
    
    
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
from applied_dq_rules