{{ config(
    materialized='table',
    tags=['intermediate', 'recent_momentum']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract daily prices and calculate 1-month (21 days) and 1-year (252 days) lags
WITH daily_quotes_lagged AS (
    SELECT 
        ticker,
        date,
        price_basic AS close_price_adj,
        
        -- Price and Date from 1 month ago (~21 trading days)
        LAG(price_basic, 21) OVER w_ticker AS price_t_21,
        LAG(date, 21) OVER w_ticker AS date_t_21,
        
        -- Price and Date from 1 year ago (~252 trading days)
        LAG(price_basic, 252) OVER w_ticker AS price_t_252,
        LAG(date, 252) OVER w_ticker AS date_t_252

    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker != 'VNINDEX'
    WINDOW w_ticker AS (PARTITION BY ticker ORDER BY date ASC)
),

-- STEP 2: Calculate 12M-1M Momentum and check data freshness
calc_recent_momentum AS (
    SELECT 
        *,
        -- AQR's 12M-1M Momentum: (Price at T-1 month / Price at T-12 months) - 1
        (price_t_21 / NULLIF(price_t_252, 0)) - 1 AS momentum_recent,
        
        -- Calculate days since last trade for freshness check
        DATE_DIFF('day', CAST(date AS DATE), CURRENT_DATE) AS days_since_last_trade
    FROM daily_quotes_lagged
),

-- STEP 3: Filter for the most recent trading day per ticker
latest_state AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
    FROM calc_recent_momentum
),

-- STEP 4: Apply inline Data Quality Rules specific to Recent Momentum
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. Freshness Check: Suspended or no trades for > 10 days
                CASE WHEN days_since_last_trade > 10 THEN 'Err: Stale Data (Suspended or no trades > 10 days)' ELSE NULL END,
                
                -- 2. Ensure 1-month benchmark exists
                CASE WHEN price_t_21 IS NULL OR price_t_21 = 0 THEN 'Err: Missing 1-month benchmark (price_t_21 is null or 0)' ELSE NULL END,
                
                -- 3. Ensure 1-year history exists
                CASE WHEN price_t_252 IS NULL OR price_t_252 = 0 THEN 'Err: Insufficient 1-year history (price_t_252 is null or 0)' ELSE NULL END,
                
                -- 4. Final validation
                CASE WHEN momentum_recent IS NULL THEN 'Err: Momentum calculation failed (momentum_recent is null)' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM latest_state
    WHERE rn = 1
)

-- STEP 5: Final Selection and Status Resolution
SELECT 
    ticker,
    date AS last_trade_date,
    days_since_last_trade,
    date_t_21,
    price_t_21,
    date_t_252,
    price_t_252,
    momentum_recent,
    
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