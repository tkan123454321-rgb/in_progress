{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_growth']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract qualified raw growth metrics
WITH base_metrics AS (
    SELECT * FROM {{ ref('int_qmj_growth') }}
    WHERE status = 'qualified'
),

-- STEP 2: Rank each growth component cross-sectionally per quarter
ranked_growth AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- Ranks (Ascending: Lower metric = Lower rank)
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_gpoa ASC) AS delta_gpoa_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_roe ASC) AS delta_roe_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_roa ASC) AS delta_roa_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_cfoa ASC) AS delta_cfoa_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_gmar ASC) AS delta_gmar_rank

    FROM base_metrics
),

-- STEP 3: Standardize ranks into Z-Scores (Mean = 0, StdDev = 1)
z_growth_components AS (
    SELECT 
        *,
        (delta_gpoa_rank - AVG(delta_gpoa_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(delta_gpoa_rank) OVER w_qtr, 0) AS z_delta_gpoa,
        (delta_roe_rank - AVG(delta_roe_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(delta_roe_rank) OVER w_qtr, 0) AS z_delta_roe,
        (delta_roa_rank - AVG(delta_roa_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(delta_roa_rank) OVER w_qtr, 0) AS z_delta_roa,
        (delta_cfoa_rank - AVG(delta_cfoa_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(delta_cfoa_rank) OVER w_qtr, 0) AS z_delta_cfoa,
        (delta_gmar_rank - AVG(delta_gmar_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(delta_gmar_rank) OVER w_qtr, 0) AS z_delta_gmar
    FROM ranked_growth
    WINDOW w_qtr AS (PARTITION BY absolute_quarter)
),

-- STEP 4: Apply Data Quality Rules to ensure all Z-Scores are calculated
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN z_delta_gpoa IS NULL THEN 'z_delta_gpoa is null' ELSE NULL END,
                CASE WHEN z_delta_roe IS NULL THEN 'z_delta_roe is null' ELSE NULL END,
                CASE WHEN z_delta_roa IS NULL THEN 'z_delta_roa is null' ELSE NULL END,
                CASE WHEN z_delta_cfoa IS NULL THEN 'z_delta_cfoa is null' ELSE NULL END,
                CASE WHEN z_delta_gmar IS NULL THEN 'z_delta_gmar is null' ELSE NULL END
             ), 
        '') AS unqualified_reason
    FROM z_growth_components
)

-- STEP 5: Final Selection, Status Resolution, and Audit Injection
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    -- Output the 5 standardized Z-Scores
    z_delta_gpoa, 
    z_delta_roe, 
    z_delta_roa, 
    z_delta_cfoa, 
    z_delta_gmar,
    
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