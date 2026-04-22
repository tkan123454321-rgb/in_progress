{{ config(
    materialized='table',
    tags=['intermediate', 'qmj', 'growth', 'z_score']
) }}

WITH base_metrics AS (
    -- Lấy nguyên liệu đã được làm sạch và đủ điều kiện từ bảng Growth
    SELECT * FROM {{ ref('int_qmj_growth') }}
    WHERE status = 'qualified'
),

ranked_growth AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- 1. Delta GPOA
        delta_gpoa,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_gpoa ASC) AS delta_gpoa_rank,
        
        -- 2. Delta ROE
        delta_roe,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_roe ASC) AS delta_roe_rank,
        
        -- 3. Delta ROA
        delta_roa,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_roa ASC) AS delta_roa_rank,
        
        -- 4. Delta CFOA
        delta_cfoa,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_cfoa ASC) AS delta_cfoa_rank,
        
        -- 5. Delta GMAR
        delta_gmar,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY delta_gmar ASC) AS delta_gmar_rank

    FROM base_metrics
),

z_growth as(
    SELECT 
        *,
        -- Z-Score cho Delta GPOA
        (delta_gpoa_rank - AVG(delta_gpoa_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(delta_gpoa_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_delta_gpoa,
        
        -- Z-Score cho Delta ROE
        (delta_roe_rank - AVG(delta_roe_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(delta_roe_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_delta_roe,
        
        -- Z-Score cho Delta ROA
        (delta_roa_rank - AVG(delta_roa_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(delta_roa_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_delta_roa,

        -- Z-Score cho Delta CFOA
        (delta_cfoa_rank - AVG(delta_cfoa_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(delta_cfoa_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_delta_cfoa,

        -- Z-Score cho Delta GMAR
        (delta_gmar_rank - AVG(delta_gmar_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(delta_gmar_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_delta_gmar

    FROM ranked_growth
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    z_delta_gpoa, 
    z_delta_roe, 
    z_delta_roa, 
    z_delta_cfoa, 
    z_delta_gmar,
    
    {{ check_qmj_z_score_column('growth') }} AS unqualified_reason,
    
    CASE 
        WHEN {{ check_qmj_z_score_column('growth') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status
    
    -- Thêm các cột audit để track dữ liệu
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
FROM z_growth