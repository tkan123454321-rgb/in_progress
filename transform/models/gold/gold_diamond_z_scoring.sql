{{ config(
    materialized='table',
    tags=['gold', 'diamond_scoring']
) }}

-- 1. LẤY HÀNG ĐÃ QUA KIỂM DUYỆT TỪ CÁC BẢNG THÀNH PHẦN
WITH qmj_qualified AS (
    SELECT 
        ticker, year, quarter, absolute_quarter, 
        qmj_score 
    FROM {{ ref('gold_qmj_z_final') }}
    WHERE status = 'qualified'
),

val_mom_qualified AS (
    SELECT 
        ticker, year, quarter, absolute_quarter, 
        z_value, z_momentum 
    FROM {{ ref('gold_value_and_momentum_z') }}
    WHERE status = 'qualified'
),

-- 2. HỘI QUÂN (FULL OUTER JOIN ĐỂ GIỮ VẾT TRUY VẤT)
joined_universe AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        q.qmj_score,
        vm.z_value,
        vm.z_momentum,
        -- Tính tổng thô (Sẽ ra NULL nếu thiếu bất kỳ mảnh ghép nào)
        (q.qmj_score + vm.z_value + vm.z_momentum) AS raw_diamond_sum
    FROM qmj_qualified q
    FULL OUTER JOIN val_mom_qualified vm 
        USING (ticker, year, quarter, absolute_quarter)
),

-- 3. XẾP HẠNG "VÔ TRÙNG" (Bí kíp: Chỉ Rank thằng đủ chân, thằng thiếu cho NULL Rank)
final_ranking AS (
    SELECT 
        *,
        -- Chỉ những ông raw_diamond_sum NOT NULL mới được vào bảng xếp hạng
        CASE 
            WHEN raw_diamond_sum IS NOT NULL 
            THEN RANK() OVER (PARTITION BY absolute_quarter ORDER BY raw_diamond_sum ASC) 
            ELSE NULL 
        END AS final_rank,
        
        -- Macro truy vết lỗi nằm ở đây
        {{ check_diamond_score_column('diamond') }} AS unqualified_reason
    FROM joined_universe
),

-- 4. ĐÚC KIM CƯƠNG (Z-SCORE)
-- Hàm AVG và STDDEV sẽ tự động lờ đi các final_rank bị NULL
final_z_score AS (
    SELECT 
        *,
        (final_rank - AVG(final_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(final_rank) OVER (PARTITION BY absolute_quarter), 0) AS diamond_score
    FROM final_ranking
)

-- 5. OUTPUT CUỐI CÙNG: KHÔNG DÙNG WHERE ĐỂ TRUY VẾT
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    qmj_score AS quality_z,
    z_value AS value_z,
    z_momentum AS momentum_z,
    
    diamond_score, -- Thằng nào unqualified thì cái này sẽ NULL

    -- Phân loại trạng thái đúng như bác yêu cầu
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified' 
        ELSE 'unqualified' 
    END AS status,
    unqualified_reason

    -- Audit Columns
    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM final_z_score
ORDER BY absolute_quarter DESC, diamond_score DESC