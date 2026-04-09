{{ config(
    materialized='table',
    tags=['intermediate', 'value_scoring']
) }}

WITH lagged_data AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,
        
        -- 1. Vốn hóa hiện tại (T)
        market_cap,
        
        -- 2. Book Equity chuẩn (T - 6 tháng) - Không cần COALESCE
        LAG(total_equity - minority_interest - preferred_stock, 2) 
            OVER (PARTITION BY ticker ORDER BY absolute_quarter) AS book_equity_6m_lag,
            
        -- 3. Bắt Gap để check dòng bị lag có chuẩn 2 quý không
        (absolute_quarter - LAG(absolute_quarter, 2) OVER (PARTITION BY ticker ORDER BY absolute_quarter)) AS gap_6m
        
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),

calc_value AS (
    SELECT 
        *,
        -- 4. Tính Value Score thô (BE/ME)
        (book_equity_6m_lag / NULLIF(market_cap, 0)) AS value_raw_score
    FROM lagged_data
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    gap_6m,
    market_cap,
    book_equity_6m_lag,
    value_raw_score,

    -- Bắt lỗi bằng Macro
    {{ check_value_and_momentum_column('value') }} AS unqualified_reason,
    CASE 
        WHEN {{ check_value_and_momentum_column('value') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Audit columns
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM calc_value