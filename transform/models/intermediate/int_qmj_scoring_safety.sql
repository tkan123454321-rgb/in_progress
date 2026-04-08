{{ config(
    materialized='table',
    tags=['gold', 'qmj_scoring_safety']
) }}

WITH base_metrics AS (
    -- Trỏ vào bảng Safety thô bác vừa làm xong
    SELECT * FROM {{ ref('int_qmj_safety') }}
    WHERE status = 'qualified'
),

ranked_safety AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- 1. BAB (Betting Against Beta)
        bab_score,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY bab_score ASC) AS bab_rank,
        
        -- 2. LEV (Leverage)
        lev_score,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY lev_score ASC) AS lev_rank,
        
        -- 3. Ohlson O-Score
        ohlson_o_score,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY ohlson_o_score ASC) AS o_rank,
        
        -- 4. Altman Z-Score
        altman_z_score,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY altman_z_score ASC) AS z_rank,
        
        -- 5. EVOL (Earnings Volatility)
        evol_score,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY evol_score ASC) AS evol_rank

    FROM base_metrics
),

z_safety_components AS (
    SELECT 
        *,
        -- Z-Score cho BAB
        (bab_rank - AVG(bab_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(bab_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_bab,
        
        -- Z-Score cho LEV
        (lev_rank - AVG(lev_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(lev_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_lev,
        
        -- Z-Score cho O-Score
        (o_rank - AVG(o_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(o_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_o,

        -- Z-Score cho Altman Z-Score
        (z_rank - AVG(z_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(z_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_z,

        -- Z-Score cho EVOL
        (evol_rank - AVG(evol_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(evol_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_evol

    FROM ranked_safety
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    -- Chỉ lôi 5 biến Z-score ra
    z_bab, 
    z_lev, 
    z_o, 
    z_z, 
    z_evol,

    -- Bắt lỗi bằng Macro
    {{ check_qmj_z_score_column('safety') }} AS unqualified_reason,
    CASE 
        WHEN {{ check_qmj_z_score_column('safety') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Thêm các cột audit để track dữ liệu
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM z_safety_components