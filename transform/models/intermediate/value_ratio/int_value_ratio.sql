{{ config(
    materialized='table',
    tags=['intermediate', 'historical_value']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}
-- STEP 1: Extract fundamentals and calculate 6-month (2 quarters) lagged Book Equity
WITH historical_lags AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- Historical Market Cap (T)
        market_cap,
        
        -- Standard Book Equity (T - 6 months)
        LAG(total_equity - minority_interest - preferred_stock, 2) OVER w_ticker AS book_equity_6m_lag,
            
        -- Gap check to ensure the lag is exactly 2 quarters
        (absolute_quarter - LAG(absolute_quarter, 2) OVER w_ticker) AS gap_6m
        
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
    WINDOW w_ticker AS (PARTITION BY ticker ORDER BY absolute_quarter)
),

-- STEP 2: Calculate the Raw Value Score
raw_value_calculation AS (
    SELECT 
        *,
        -- Value Score = Book Equity (t-2) / Market Equity (t)
        (book_equity_6m_lag / NULLIF(market_cap, 0)) AS value_raw_score
    FROM historical_lags
),

-- STEP 3: Apply Data Quality Rules specific to Value
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Continuity Check: Must have exactly a 2-quarter gap
                CASE WHEN gap_6m IS NULL OR gap_6m != 2 THEN 'Err: Missing 6-month lag for Book Equity (Gap != 2)' ELSE NULL END,
                
                -- Null check for historical book equity
                CASE WHEN book_equity_6m_lag IS NULL THEN 'Err: Lagged Book Equity is null' ELSE NULL END,

                -- Null check for value_raw_score
                CASE WHEN value_raw_score IS NULL THEN 'Err: Value Score is null' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM raw_value_calculation
)
-- STEP 4: Final Selection and Status Resolution
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    gap_6m,
    market_cap,
    book_equity_6m_lag,
    value_raw_score,

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