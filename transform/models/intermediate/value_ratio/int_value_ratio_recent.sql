{{ config(
    materialized='table',
    tags=['intermediate', 'recent_value']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract current market capitalization and freshness from Gold layer
WITH gold_market_cap AS (
    SELECT 
        ticker,
        market_cap,
        gold_updated_at,
        DATE_DIFF('day', CAST(gold_updated_at AS DATE), CURRENT_DATE) AS days_since_update
    FROM {{ ref('gold_dim_company') }}
    WHERE ticker != 'VNINDEX'
),

-- STEP 2: Extract the most recent fundamental metrics (Book Equity) per ticker
latest_fundamentals AS (
    SELECT 
        ticker,
        year AS report_year,
        quarter AS report_quarter,
        absolute_quarter AS latest_absolute_quarter,
        
        -- Core Book Equity
        (total_equity - minority_interest - preferred_stock) AS latest_book_equity,
        
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY absolute_quarter DESC) as rn_q
        
    FROM {{ ref('int_ttm_metrics') }} 
    WHERE ttm_status = 'valid_ttm'
),

-- STEP 3: Combine market data with fundamentals and calculate reporting delays
live_value_calculation AS (
    SELECT 
        c.ticker,
        c.gold_updated_at AS last_market_cap_update,
        c.days_since_update,
        c.market_cap,
        
        f.report_year,
        f.report_quarter,
        f.latest_absolute_quarter,
        f.latest_book_equity,

        -- Calculate current absolute quarter based on system date
        (EXTRACT(YEAR FROM CURRENT_DATE) * 4 + EXTRACT(QUARTER FROM CURRENT_DATE)) AS current_absolute_quarter,
        
        -- Calculate delay in quarters
        ((EXTRACT(YEAR FROM CURRENT_DATE) * 4 + EXTRACT(QUARTER FROM CURRENT_DATE)) - f.latest_absolute_quarter) AS quarters_delayed,

        -- Value Score (Book Equity / Market Cap)
        (f.latest_book_equity / NULLIF(c.market_cap, 0)) AS value_recent_score

    FROM gold_market_cap c
    INNER JOIN latest_fundamentals f 
        ON c.ticker = f.ticker 
        AND f.rn_q = 1
),

-- STEP 4: Apply inline Data Quality Rules specific to Recent Value
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. Freshness filter for Market Cap (> 10 days = unqualified)
                CASE WHEN days_since_update > 10 THEN 'Err: Stale Data in gold_dim_company (> 10 days)' ELSE NULL END,
                
                -- 2. Freshness filter for Fundamentals (> 2 quarters delayed = unqualified)
                CASE WHEN quarters_delayed > 2 THEN 'Err: Stale Fundamental Data (Delayed > 2 Quarters)' ELSE NULL END,
                
                -- 3. Validation for calculated score
                CASE WHEN value_recent_score IS NULL THEN 'Err: Invalid Value Recent Score' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM live_value_calculation
)

-- STEP 5: Final Selection and Status Resolution
SELECT 
    ticker,
    last_market_cap_update,
    days_since_update,
    report_year,
    report_quarter,
    latest_absolute_quarter,
    current_absolute_quarter,
    quarters_delayed,
    market_cap,
    latest_book_equity,
    value_recent_score,

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