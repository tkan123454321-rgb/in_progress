{{ config(
    materialized='table',
    enabled=true
) }}

WITH base_metrics AS (
    -- Lấy nguyên liệu đã được làm sạch và đủ điều kiện
    SELECT * FROM {{ ref('int_qmj_profitability') }}
    WHERE status = 'qualified'
),

ranked_profitability AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- 1. GPOA (Gross Profits-to-Assets) - Biên LN gộp trên Tài sản
        gpoa,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY gpoa ASC) AS gpoa_rank,
        
        -- 2. ROE (Return on Equity) - Tỷ suất sinh lời trên Vốn chủ
        roe,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY roe ASC) AS roe_rank,
        
        -- 3. ROA (Return on Assets) - Tỷ suất sinh lời trên Tổng tài sản
        roa,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY roa ASC) AS roa_rank,
        
        -- 4. CFOA (Cash Flow to Assets) - Dòng tiền HĐKD trên Tài sản
        cfoa,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY cfoa ASC) AS cfoa_rank,
        
        -- 5. GMAR (Gross Margin) - Biên lợi nhuận gộp
        gmar,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY gmar ASC) AS gmar_rank,
        
        -- 6. ACC (Accruals) - Biến động dồn tích (Đã đảo dấu thành DP - dWC)
        acc,
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY acc ASC) AS acc_rank

    FROM base_metrics
),


z_profitability as(
    SELECT 
        *,
        (gpoa_rank - AVG(gpoa_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(gpoa_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_gpoa,
        
        -- Z-Score cho ROE
        (roe_rank - AVG(roe_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(roe_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_roe,
        
        -- Z-Score cho ROA
        (roa_rank - AVG(roa_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(roa_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_roa,

        -- Z-Score cho CFOA
        (cfoa_rank - AVG(cfoa_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(cfoa_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_cfoa,

        -- Z-Score cho GMAR
        (gmar_rank - AVG(gmar_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(gmar_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_gmar,

        -- Z-Score cho ACC
        (acc_rank - AVG(acc_rank) OVER (PARTITION BY absolute_quarter)) 
        / NULLIF(STDDEV_SAMP(acc_rank) OVER (PARTITION BY absolute_quarter), 0) AS z_acc

    FROM ranked_profitability
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    z_gpoa, z_roe, z_roa, z_cfoa, z_gmar, z_acc,
    {{ check_qmj_z_score_column('profitability') }} AS unqualified_reason,
    CASE 
        WHEN {{ check_qmj_z_score_column('profitability') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status
    -- Thêm các cột audit để track dữ liệu
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
FROM z_profitability