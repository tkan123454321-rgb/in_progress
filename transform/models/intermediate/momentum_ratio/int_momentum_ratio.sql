{{ config(
    materialized='table',
    tags=['intermediate', 'historical_momentum']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}
-- STEP 1: Extract End-of-Month (EOM) prices for stocks only
WITH eom_prices AS (
    SELECT 
        ticker,
        date,
        year,
        quarter,
        month,
        absolute_month,
        absolute_quarter,
        price_basic AS close_price_adj,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, absolute_month 
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker != 'VNINDEX'
),
-- STEP 2: Filter to keep only the last trading day of each month
monthly_series AS (
    SELECT * FROM eom_prices 
    WHERE rn = 1
),
-- STEP 3: Create 1-month and 12-month lagged price points
lagged_prices AS (
    SELECT 
        ticker,
        year,
        quarter,
        month,
        absolute_month,
        absolute_quarter,
        close_price_adj,
        
        -- Price from 1 month ago (T-1)
        LAG(close_price_adj, 1) OVER w_ticker AS price_t_1,
        
        -- Price from 12 months ago (T-12)
        LAG(close_price_adj, 12) OVER w_ticker AS price_t_12,
        
        -- Gap check to enforce strict 12-month historical availability
        (absolute_month - LAG(absolute_month, 12) OVER w_ticker) AS gap_12m

    FROM monthly_series
    WINDOW w_ticker AS (PARTITION BY ticker ORDER BY absolute_month)
),

-- STEP 4: Calculate the raw 12M-1M Momentum score
calc_raw_momentum AS (
    SELECT 
        *,
        (price_t_1 / NULLIF(price_t_12, 0)) - 1 AS momentum_raw_score
    FROM lagged_prices
),

-- STEP 5: Align monthly momentum scores to quarter-ends
quarterly_alignment AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, year, quarter 
            ORDER BY month DESC
        ) AS rn_q
    FROM calc_raw_momentum
),

-- STEP 6: Apply inline Data Quality Rules specific to Historical Momentum
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Strict 12-month gap check
                CASE WHEN gap_12m IS NULL OR gap_12m != 12 THEN 'Err: Missing 12-month lag for Price (Gap != 12)' ELSE NULL END,
                
                -- Null or zero checks for lagged prices
                CASE WHEN price_t_1 IS NULL THEN 'Err: Lagged Price 1m is null' ELSE NULL END,
                CASE WHEN price_t_12 IS NULL OR price_t_12 = 0 THEN 'Err: Lagged Price 12m is null or zero' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM quarterly_alignment
    WHERE rn_q = 1
)
-- STEP 7: Final Selection and Status Resolution
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    month AS end_of_quarter_month,
    absolute_month,
    gap_12m,
    price_t_1,
    price_t_12,
    momentum_raw_score,

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