{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_scoring_safety']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract qualified raw safety metrics from the ephemeral layer
WITH base_metrics AS (
    SELECT * FROM {{ ref('int_qmj_safety') }}
    WHERE status = 'qualified'
),

-- STEP 2: Rank each component cross-sectionally per quarter
ranked_safety AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- 1. BAB (Betting Against Beta)
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY bab_score ASC) AS bab_rank,
        
        -- 2. LEV (Leverage)
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY lev_score ASC) AS lev_rank,
        
        -- 3. Ohlson O-Score
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY ohlson_o_score ASC) AS o_rank,
        
        -- 4. Altman Z-Score
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY altman_z_score ASC) AS z_rank,
        
        -- 5. EVOL (Earnings Volatility)
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY evol_score ASC) AS evol_rank

    FROM base_metrics
),
-- STEP 3: Standardize ranks into Z-Scores
z_safety_components AS (
    SELECT 
        *,
        (bab_rank - AVG(bab_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(bab_rank) OVER w_qtr, 0) AS z_bab,
        (lev_rank - AVG(lev_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(lev_rank) OVER w_qtr, 0) AS z_lev,
        (o_rank - AVG(o_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(o_rank) OVER w_qtr, 0) AS z_o,
        (z_rank - AVG(z_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(z_rank) OVER w_qtr, 0) AS z_z,
        (evol_rank - AVG(evol_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(evol_rank) OVER w_qtr, 0) AS z_evol
    FROM ranked_safety
    WINDOW w_qtr AS (PARTITION BY absolute_quarter)
),

-- STEP 4: Apply Data Quality Rules to ensure all Z-Scores are calculated
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN z_bab IS NULL THEN 'z_bab is null' ELSE NULL END,
                CASE WHEN z_lev IS NULL THEN 'z_lev is null' ELSE NULL END,
                CASE WHEN z_o IS NULL THEN 'z_o is null' ELSE NULL END,
                CASE WHEN z_z IS NULL THEN 'z_z is null' ELSE NULL END,
                CASE WHEN z_evol IS NULL THEN 'z_evol is null' ELSE NULL END
             ), 
        '') AS unqualified_reason
    FROM z_safety_components
)

-- STEP 5: Final Selection, Status Resolution, and Audit Injection
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    -- Output the 5 standardized Z-Scores
    z_bab, 
    z_lev, 
    z_o, 
    z_z, 
    z_evol,

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