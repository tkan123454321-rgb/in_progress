{{ config(
    materialized='table',
    enabled=true
) }}


WITH intermediate_data AS (
    SELECT * FROM {{ ref('int_qmj_scoring_profitability') }}
    WHERE status = 'qualified' -- Chỉ lấy hàng tuyển lên lớp Gold
),

calculate_sum AS (
    SELECT 
        *,
        (z_gpoa + z_roe + z_roa + z_cfoa + z_gmar + z_acc) AS raw_profitability_sum
    FROM intermediate_data
),

final_ranking AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY raw_profitability_sum ASC) AS final_rank
    FROM calculate_sum
)

SELECT
    ticker,
    year,
    quarter,
    absolute_quarter,
    z_gpoa,
    z_roe,
    z_roa,
    z_cfoa,
    z_gmar,
    z_acc,
    -- Chốt hạ điểm Profitability chuẩn AQR
    (final_rank - AVG(final_rank) OVER (PARTITION BY absolute_quarter)) 
    / NULLIF(STDDEV_SAMP(final_rank) OVER (PARTITION BY absolute_quarter), 0) AS qmj_profitability_score
    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM final_ranking