{{ config(
    materialized='table',
    tags=['gold', 'qmj_final']
) }}

-- 1. GẠN ĐỤC KHƠI TRONG
WITH profitability AS (
    SELECT * FROM {{ ref('gold_qmj_z_profitability') }} 
),

growth AS (
    SELECT * FROM {{ ref('gold_qmj_z_growth') }} 
),

safety AS (
    SELECT * FROM {{ ref('gold_qmj_z_safety') }} 
),

-- 2. HỘI QUÂN (Sạch sẽ nhờ USING)
joined_scores AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        p.qmj_profitability_score,
        g.qmj_growth_score,
        s.qmj_safety_score,
        
        -- Thằng nào thiếu 1 trong 3 là NULL ngay
        (p.qmj_profitability_score + g.qmj_growth_score + s.qmj_safety_score) AS raw_qmj_sum,
        
        {{ check_qmj_z_score_column('qmj_final') }} AS unqualified_reason

    FROM profitability p
    FULL OUTER JOIN growth g USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN safety s USING (ticker, year, quarter, absolute_quarter)
),

-- 3. XẾP HẠNG (Chỉ Rank những ông có tổng điểm)
final_ranking AS (
    SELECT 
        *,
        CASE 
            WHEN raw_qmj_sum IS NOT NULL 
            THEN RANK() OVER (PARTITION BY absolute_quarter ORDER BY raw_qmj_sum ASC) 
            ELSE NULL 
        END AS final_rank
    FROM joined_scores
),

-- 4. CHỐT ĐIỂM Z-SCORE
final_z_score AS (
    SELECT 
        *,
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
    qmj_score,

    CASE WHEN unqualified_reason IS NULL THEN 'qualified' ELSE 'unqualified' END AS status,
    unqualified_reason

    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM final_z_score
ORDER BY absolute_quarter DESC, qmj_score DESC