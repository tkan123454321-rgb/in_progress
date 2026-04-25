{{ config(
    materialized='table',
    tags=['gold', 'historical_value_and_momentum_z']
) }}

{% set audit_cols = get_audit_columns('gold') %}

-- STEP 1: Extract qualified historical scores for Value and Momentum
WITH base_value AS (
    SELECT * FROM {{ ref('int_value_ratio') }}
    WHERE status = 'qualified' 
),

base_momentum AS (
    SELECT * FROM {{ ref('int_momentum_ratio') }}
    WHERE status = 'qualified'
),

-- STEP 2: Combine metrics cleanly using USING clause
joined_metrics AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        v.value_raw_score,
        m.momentum_raw_score
    FROM base_value v
    FULL OUTER JOIN base_momentum m USING (ticker, year, quarter, absolute_quarter)
),

-- STEP 3: Rank metrics cross-sectionally per quarter
ranked_metrics AS (
    SELECT 
        *,
        -- Rank Value: Lower score = Lower Rank
        CASE 
            WHEN value_raw_score IS NOT NULL 
            THEN RANK() OVER w_qtr_val 
            ELSE NULL 
        END AS value_rank,
        
        -- Rank Momentum: Lower score = Lower Rank
        CASE 
            WHEN momentum_raw_score IS NOT NULL 
            THEN RANK() OVER w_qtr_mom 
            ELSE NULL 
        END AS momentum_rank
    FROM joined_metrics
    WINDOW 
        w_qtr_val AS (PARTITION BY absolute_quarter ORDER BY value_raw_score ASC),
        w_qtr_mom AS (PARTITION BY absolute_quarter ORDER BY momentum_raw_score ASC)
),

-- STEP 4: Standardize ranks into Z-Scores per quarter
z_score_components AS (
    SELECT 
        *,
        -- Z-Score for Historical Value
        (value_rank - AVG(value_rank) OVER w_qtr) 
        / NULLIF(STDDEV_SAMP(value_rank) OVER w_qtr, 0) AS z_value,
        
        -- Z-Score for Historical Momentum
        (momentum_rank - AVG(momentum_rank) OVER w_qtr) 
        / NULLIF(STDDEV_SAMP(momentum_rank) OVER w_qtr, 0) AS z_momentum

    FROM ranked_metrics
    WINDOW w_qtr AS (PARTITION BY absolute_quarter)
),

-- STEP 5: Apply inline Data Quality Rules
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN z_value IS NULL THEN 'z_value is null' ELSE NULL END,
                CASE WHEN z_momentum IS NULL THEN 'z_momentum is null' ELSE NULL END
             ), 
        '') AS unqualified_reason
    FROM z_score_components
)

-- STEP 6: Final Selection, Status Resolution, and Audit Injection
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    value_raw_score,
    momentum_raw_score,
    
    z_value, 
    z_momentum,

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