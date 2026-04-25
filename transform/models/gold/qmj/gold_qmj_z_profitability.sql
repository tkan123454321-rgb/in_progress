{{ config(
    materialized = 'table',
    tags = ['gold', 'qmj_profitability']
) }} 

{% set audit_cols = get_audit_columns('gold') %} 

-- STEP 1: Extract qualified Z-scores from the intermediate profitability layer
WITH intermediate_data AS (
    SELECT *
    FROM {{ ref('int_qmj_scoring_profitability') }}
    WHERE status = 'qualified'
),

-- STEP 2: Sum the 6 standardized profitability components
calculate_sum AS (
    SELECT 
        *,
        (z_gpoa + z_roe + z_roa + z_cfoa + z_gmar + z_acc) AS raw_profitability_sum
    FROM intermediate_data
),

-- STEP 3: Rank the aggregated profitability sums cross-sectionally per quarter
final_ranking AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY raw_profitability_sum ASC) AS final_rank
    FROM calculate_sum
)

-- STEP 4: Final Selection and Calculation of the AQR Profitability Z-Score
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    -- Retain underlying components for downstream dashboards and debugging
    z_gpoa,
    z_roe,
    z_roa,
    z_cfoa,
    z_gmar,
    z_acc,
    
    -- Final AQR Profitability Score: Z-Score of the cross-sectional ranks
    (final_rank - AVG(final_rank) OVER w_qtr) / NULLIF(STDDEV_SAMP(final_rank) OVER w_qtr, 0) AS qmj_profitability_score 
    
    -- Auto-generated audit columns for the Gold layer
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }} 
    {% endfor %}

FROM final_ranking
WINDOW w_qtr AS (PARTITION BY absolute_quarter)