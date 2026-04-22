{{ config(
    materialized='table',
    tags=['gold', 'daily_screener', 'value_and_momentum_z']
) }}

-- 1. LẤY NGUYÊN LIỆU
WITH base_value AS (
    SELECT 
        ticker,
        value_recent_score,
        last_market_cap_update
    FROM {{ ref('int_value_ratio_recent') }}
    WHERE status = 'qualified' 
),

base_momentum AS (
    SELECT 
        ticker,
        momentum_recent,
        last_trade_date
    FROM {{ ref('int_momentum_ratio_recent') }}
    WHERE status = 'qualified'
),

-- 2. HỘI QUÂN RECENT 
joined_metrics AS (
    SELECT 
        ticker,
        v.value_recent_score,
        m.momentum_recent
    FROM base_value v
    FULL OUTER JOIN base_momentum m USING (ticker)
),

-- 3. XẾP HẠNG TRÊN TOÀN THỊ TRƯỜNG HIỆN TẠI 
ranked_metrics AS (
    SELECT 
        *,
        CASE 
            WHEN value_recent_score IS NOT NULL 
            THEN RANK() OVER (ORDER BY value_recent_score ASC) 
            ELSE NULL 
        END AS value_rank,
        
        CASE 
            WHEN momentum_recent IS NOT NULL 
            THEN RANK() OVER (ORDER BY momentum_recent ASC) 
            ELSE NULL 
        END AS momentum_rank
    FROM joined_metrics
),

-- 4. CHỐT Z-SCORE
z_score_components AS (
    SELECT 
        *,
        -- Z-Score cho Value
        (value_rank - AVG(value_rank) OVER ()) 
        / NULLIF(STDDEV_SAMP(value_rank) OVER (), 0) AS z_value_recent,
        
        -- Z-Score cho Momentum
        (momentum_rank - AVG(momentum_rank) OVER ()) 
        / NULLIF(STDDEV_SAMP(momentum_rank) OVER (), 0) AS z_momentum_recent

    FROM ranked_metrics
)

SELECT 
    ticker,
    value_recent_score,
    momentum_recent,
    z_value_recent, 
    z_momentum_recent,

    -- Bắt lỗi bằng Macro 
    {{ check_value_and_momentum_z_score_column('value_momentum_z_recent') }} AS unqualified_reason,
    
    CASE 
        WHEN {{ check_value_and_momentum_z_score_column('value_momentum_z_recent') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Thêm các cột audit
    {% set audit_cols = get_audit_columns('gold') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM z_score_components