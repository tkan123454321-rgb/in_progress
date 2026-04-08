{{ config(
    materialized='table',
    enabled=true,
    tags=['gold', 'qmj', 'safety']
) }}

WITH intermediate_data AS (
    -- Lấy data từ bảng Z-Score Safety bác vừa tạo ở bước trước
    SELECT * FROM {{ ref('int_qmj_scoring_safety') }}
    WHERE status = 'qualified' -- Chỉ lấy hàng tuyển lên lớp Gold
),

calculate_sum AS (
    SELECT 
        *,
        -- Cộng tổng 5 thành phần Z-Score của Safety
        (z_bab + z_lev + z_o + z_z + z_evol) AS raw_safety_sum
    FROM intermediate_data
),

final_ranking AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY raw_safety_sum ASC) AS final_rank
    FROM calculate_sum
)

SELECT
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    -- Giữ lại các thành phần để sau này làm Dashboard hoặc Debug cho dễ
    z_bab,
    z_lev,
    z_o,
    z_z,
    z_evol,
    
    -- CHỐT HẠ: Điểm tổng Safety chuẩn AQR
    (final_rank - AVG(final_rank) OVER (PARTITION BY absolute_quarter)) 
    / NULLIF(STDDEV_SAMP(final_rank) OVER (PARTITION BY absolute_quarter), 0) AS qmj_safety_score
    
    -- Audit columns
    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM final_ranking