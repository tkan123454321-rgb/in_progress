{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_profitability']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract qualified raw profitability metrics
WITH base_metrics AS (
    SELECT * FROM {{ ref('int_qmj_profitability') }}
    WHERE status = 'qualified'
),

-- STEP 2: Rank each profitability component cross-sectionally per quarter
ranked_profitability AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- Ranks (Ascending: Lower metric = Lower rank)
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY gpoa ASC) AS gpoa_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY roe ASC) AS roe_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY roa ASC) AS roa_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY cfoa ASC) AS cfoa_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY gmar ASC) AS gmar_rank,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY acc ASC) AS acc_rank

    FROM base_metrics
),

-- STEP 3: Standardize ranks into Z-Scores (Mean = 0, StdDev = 1)
z_profitability_components AS (
    SELECT 
        *,
        (gpoa_rank - AVG(gpoa_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(gpoa_rank) OVER w_qtr, 0) AS z_gpoa,
        (roe_rank - AVG(roe_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(roe_rank) OVER w_qtr, 0) AS z_roe,
        (roa_rank - AVG(roa_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(roa_rank) OVER w_qtr, 0) AS z_roa,
        (cfoa_rank - AVG(cfoa_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(cfoa_rank) OVER w_qtr, 0) AS z_cfoa,
        (gmar_rank - AVG(gmar_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(gmar_rank) OVER w_qtr, 0) AS z_gmar,
        (acc_rank - AVG(acc_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(acc_rank) OVER w_qtr, 0) AS z_acc
    FROM ranked_profitability
    WINDOW w_qtr AS (PARTITION BY absolute_quarter)
),

-- STEP 4: Apply Data Quality Rules to ensure all Z-Scores are calculated
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN z_gpoa IS NULL THEN 'z_gpoa is null' ELSE NULL END,
                CASE WHEN z_roe IS NULL THEN 'z_roe is null' ELSE NULL END,
                CASE WHEN z_roa IS NULL THEN 'z_roa is null' ELSE NULL END,
                CASE WHEN z_cfoa IS NULL THEN 'z_cfoa is null' ELSE NULL END,
                CASE WHEN z_gmar IS NULL THEN 'z_gmar is null' ELSE NULL END,
                CASE WHEN z_acc IS NULL THEN 'z_acc is null' ELSE NULL END
             ), 
        '') AS unqualified_reason
    FROM z_profitability_components
)

-- STEP 5: Final Selection, Status Resolution, and Audit Injection
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    -- Output the 6 standardized Z-Scores
    z_gpoa,
    z_roe,
    z_roa,
    z_cfoa,
    z_gmar,
    z_acc,
    
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