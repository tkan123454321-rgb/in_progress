{{ config(
    materialized='table',
    tags=['gold', 'recent_value_and_momentum_z']
) }}

{% set audit_cols = get_audit_columns('gold') %}

-- STEP 1: Extract qualified recent scores for Value and Momentum
WITH base_value AS (
    SELECT 
        ticker,
        value_recent_score,
        last_market_cap_update
    FROM {{ ref('int_value_ratio_recent') }}
    WHERE status = 'qualified' 
),

base_momentum AS (
    SELECT 
        ticker,
        momentum_recent,
        last_trade_date
    FROM {{ ref('int_momentum_ratio_recent') }}
    WHERE status = 'qualified'
),

-- STEP 2: Combine metrics (Full Outer Join to retain all valid tickers)
joined_metrics AS (
    SELECT 
        ticker,
        v.value_recent_score,
        m.momentum_recent
    FROM base_value v
    FULL OUTER JOIN base_momentum m USING (ticker)
),

-- STEP 3: Rank metrics cross-sectionally across the entire current market
-- Note: No partition clause since this is a live snapshot of the whole market
ranked_metrics AS (
    SELECT 
        *,
        CASE 
            WHEN value_recent_score IS NOT NULL 
            THEN RANK() OVER (ORDER BY value_recent_score ASC) 
            ELSE NULL 
        END AS value_rank,
        
        CASE 
            WHEN momentum_recent IS NOT NULL 
            THEN RANK() OVER (ORDER BY momentum_recent ASC) 
            ELSE NULL 
        END AS momentum_rank
    FROM joined_metrics
),
-- STEP 4: Standardize ranks into Z-Scores
z_score_components AS (
    SELECT 
        *,
        -- Z-Score for Live Value
        (value_rank - AVG(value_rank) OVER ()) 
        / NULLIF(STDDEV_SAMP(value_rank) OVER (), 0) AS z_value_recent,
        
        -- Z-Score for Live Momentum
        (momentum_rank - AVG(momentum_rank) OVER ()) 
        / NULLIF(STDDEV_SAMP(momentum_rank) OVER (), 0) AS z_momentum_recent

    FROM ranked_metrics
),

-- STEP 5: Apply inline Data Quality Rules
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN z_value_recent IS NULL THEN 'z_value_recent is null' ELSE NULL END,
                CASE WHEN z_momentum_recent IS NULL THEN 'z_momentum_recent is null' ELSE NULL END
             ), 
        '') AS unqualified_reason
    FROM z_score_components
)

-- STEP 6: Final Selection, Status Resolution, and Audit Injection
SELECT 
    ticker,
    value_recent_score,
    momentum_recent,
    z_value_recent, 
    z_momentum_recent,
    
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