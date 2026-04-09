{{ config(
    materialized='table',
    tags=['gold', 'value_and_momentum_z']
) }}

-- 1. LẤY NGUYÊN LIỆU (Giữ nguyên hoặc lọc qualified tùy bác, ở đây tôi để thoáng để FULL JOIN phát huy tác dụng)
WITH base_value AS (
    SELECT * FROM {{ ref('int_value_ratio') }}
    -- Thường thì ở tầng Gold mình chỉ lấy hàng qualified để tính Z-Score cho chuẩn
    WHERE status = 'qualified' 
),

base_momentum AS (
    SELECT * FROM {{ ref('int_momentum_ratio') }}
    WHERE status = 'qualified'
),

-- 2. HỘI QUÂN (Dùng FULL OUTER JOIN + USING để code sạch bóng)
joined_metrics AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        v.value_raw_score,
        m.momentum_raw_score
    FROM base_value v
    FULL OUTER JOIN base_momentum m USING (ticker, year, quarter, absolute_quarter)
),

-- 3. XẾP HẠNG "VÔ TRÙNG" (Chỉ Rank những thằng không NULL)
ranked_metrics AS (
    SELECT 
        *,
        -- Rank Value: Càng cao (rẻ) càng tốt -> DESC
        CASE 
            WHEN value_raw_score IS NOT NULL 
            THEN RANK() OVER (PARTITION BY absolute_quarter ORDER BY value_raw_score DESC) 
            ELSE NULL 
        END AS value_rank,
        
        -- Rank Momentum: Càng cao (đà mạnh) càng tốt -> DESC
        CASE 
            WHEN momentum_raw_score IS NOT NULL 
            THEN RANK() OVER (PARTITION BY absolute_quarter ORDER BY momentum_raw_score DESC) 
            ELSE NULL 
        END AS momentum_rank
    FROM joined_metrics
),

-- 4. CHỐT Z-SCORE (Hàm aggregate tự động bỏ qua NULL trong Rank)
z_score_components AS (
    SELECT 
        *,
        -- Z-Score cho Value
        (value_rank - AVG(value_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(value_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_value,
        
        -- Z-Score cho Momentum
        (momentum_rank - AVG(momentum_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(momentum_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_momentum

    FROM ranked_metrics
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    value_raw_score,
    momentum_raw_score,
    
    -- Hai biến Z-score đã được "vô trùng"
    z_value, 
    z_momentum,

    -- Bắt lỗi bằng Macro (Nhớ check lại macro value_momentum_z đã update chưa nhé bác)
    {{ check_value_and_momentum_z_score_column('value_momentum_z') }} AS unqualified_reason,
    CASE 
        WHEN {{ check_value_and_momentum_z_score_column('value_momentum_z') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Thêm các cột audit
    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM z_score_components