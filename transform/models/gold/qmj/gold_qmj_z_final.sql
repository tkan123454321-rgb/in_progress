{{ config(
    materialized='table',
    tags=['gold', 'qmj_final']
) }}

{% set audit_cols = get_audit_columns('gold') %}

-- STEP 1: Extract the 3 core pillars of Quality from the Gold layer
WITH gold_profitability AS (
    SELECT * FROM {{ ref('gold_qmj_z_profitability') }} 
),

gold_growth AS (
    SELECT * FROM {{ ref('gold_qmj_z_growth') }} 
),

gold_safety AS (
    SELECT * FROM {{ ref('gold_qmj_z_safety') }} 
),

-- STEP 2: Combine the 3 pillars (Clean join using USING)
joined_scores AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        p.qmj_profitability_score,
        g.qmj_growth_score,
        s.qmj_safety_score,
        
        -- The sum will automatically be NULL if any of the 3 components is NULL
        (p.qmj_profitability_score + g.qmj_growth_score + s.qmj_safety_score) AS raw_qmj_sum

    FROM gold_profitability p
    FULL OUTER JOIN gold_growth g USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN gold_safety s USING (ticker, year, quarter, absolute_quarter)
),

-- STEP 3: Rank the aggregated QMJ sums cross-sectionally per quarter
final_ranking AS (
    SELECT 
        *,
        -- Rank only valid records; missing components result in a NULL rank
        CASE 
            WHEN raw_qmj_sum IS NOT NULL 
            THEN RANK() OVER w_qtr_asc
            ELSE NULL 
        END AS final_rank
    FROM joined_scores
    WINDOW w_qtr_asc AS (PARTITION BY absolute_quarter ORDER BY raw_qmj_sum ASC)
),

-- STEP 4: Calculate the ultimate AQR QMJ Score (Z-Score of the ranks)
final_z_score AS (
    SELECT 
        *,
        (final_rank - AVG(final_rank) OVER w_qtr) 
        / NULLIF(STDDEV_SAMP(final_rank) OVER w_qtr, 0) AS qmj_score
    FROM final_ranking
    WINDOW w_qtr AS (PARTITION BY absolute_quarter)
),

-- STEP 5: Apply inline Data Quality Rules for QMJ Final
applied_dq_rules AS (
    SELECT 
        *,
        -- Inline DQ Check: Ensure all 3 quality pillars are present
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN qmj_profitability_score IS NULL THEN 'Missing Profitability Score' ELSE NULL END,
                CASE WHEN qmj_growth_score IS NULL THEN 'Missing Growth Score' ELSE NULL END,
                CASE WHEN qmj_safety_score IS NULL THEN 'Missing Safety Score' ELSE NULL END
             ), 
        '') AS unqualified_reason
    FROM final_z_score
)

-- STEP 6: Final Selection, Status Resolution, and Audit Injection
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    qmj_profitability_score,
    qmj_growth_score,
    qmj_safety_score,
    qmj_score,

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
ORDER BY absolute_quarter DESC, qmj_score DESC