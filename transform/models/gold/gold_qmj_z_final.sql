{{ config(
    materialized='table',
    tags=['gold', 'qmj_final']
) }}

WITH joined_scores AS (
    -- Gộp toàn bộ vũ trụ 3 bảng
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        p.qmj_profitability_score,
        g.qmj_growth_score,
        s.qmj_safety_score,
        
        -- Tính tổng thô (Thằng nào thiếu 1 trong 3 điểm thì kết quả tự động ra NULL)
        (p.qmj_profitability_score + g.qmj_growth_score + s.qmj_safety_score) AS raw_qmj_sum,
        
        -- Gọi Macro bắt lỗi truy vết
        {{ check_qmj_z_score_column('qmj_final') }} AS unqualified_reason

    FROM {{ ref('gold_qmj_z_profitability') }} p
    FULL OUTER JOIN {{ ref('gold_qmj_z_growth') }} g USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN {{ ref('gold_qmj_z_safety') }} s USING (ticker, year, quarter, absolute_quarter)
),

final_ranking AS (
    SELECT 
        *,
        -- Xếp hạng TẤT CẢ các mã (Nếu raw_qmj_sum = NULL, Rank tự động bỏ qua)
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY raw_qmj_sum ASC) AS final_rank
    FROM joined_scores
),

final_z_score AS (
    SELECT 
        *,
        -- Tính Z-Score trên TẤT CẢ các mã có Rank
        (final_rank - AVG(final_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(final_rank) OVER (PARTITION BY absolute_quarter), 0) AS qmj_score
    FROM final_ranking
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    qmj_profitability_score,
    qmj_growth_score,
    qmj_safety_score,
    
    -- Vương miện QMJ
    qmj_score,

    -- Trạng thái chốt để đưa lên Dashboard
    CASE WHEN unqualified_reason IS NULL THEN 'qualified' ELSE 'unqualified' END AS status,
    unqualified_reason

    -- Audit Columns
    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM final_z_score
ORDER BY absolute_quarter DESC, qmj_score DESC