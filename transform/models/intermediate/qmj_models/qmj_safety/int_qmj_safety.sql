{{ config(
    materialized='table',
    tags=['int', 'qmj_safety']
) }}

WITH ttm_metrics AS (
    SELECT * FROM {{ ref('int_ttm_metrics') }} 
    WHERE ttm_status = 'valid_ttm'
),

bab_score AS (
    -- Đổi tên cột year, quarter cho khớp để dùng mệnh đề USING
    SELECT 
        *
    FROM {{ ref('int_qmj_beta_final') }}
    WHERE status = 'qualified'
),

z_score AS (
    SELECT * FROM {{ ref('int_z_score') }}
    WHERE status = 'qualified'
),

o_score AS (
    SELECT * FROM {{ ref('int_o_score') }}
    WHERE status = 'qualified'
),

joined_all AS (
    -- Dùng FULL OUTER JOIN + USING: Sạch sẽ, không rớt data
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,

        b.bab_score,
        z.altman_z_score,
        o.ohlson_o_score,

        t.total_assets,
        t.net_income_parent_ttm,
        (t.total_equity - t.minority_interest - t.preferred_stock) AS book_equity,
        t.short_term_debt AS short_term_debt,
        t.long_term_debt AS long_term_debt,
        t.minority_interest AS minority_interest,
        t.preferred_stock AS preferred_stock

    FROM ttm_metrics t
    FULL OUTER JOIN bab_score b USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN z_score z USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN o_score o USING (ticker, year, quarter, absolute_quarter)
),

calc_lev_and_roe AS (
    SELECT 
        *,
        -- 1. Tính LEV Score
        -(short_term_debt + long_term_debt + minority_interest + preferred_stock) / NULLIF(total_assets, 0) AS lev_score,
        
        -- 2. Tính ROE để làm đầu vào cho EVOL
        net_income_parent_ttm / NULLIF(book_equity, 0) AS roe
    FROM joined_all
),

calc_evol AS (
    SELECT 
        *,
        -- Tính biến động ROE trong 16 quý
        STDDEV_SAMP(roe) OVER (
            PARTITION BY ticker 
            ORDER BY absolute_quarter 
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) AS evol_raw,
        
        COUNT(roe) OVER (
            PARTITION BY ticker 
            ORDER BY absolute_quarter 
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) AS count_roe_quarters
    FROM calc_lev_and_roe
),

apply_dq AS (
    SELECT 
        *,
        -- Gán điểm EVOL (đảo dấu)
        (-1 * evol_raw) AS evol_score,
        
        -- Gọi Macro để bắt lỗi toàn bộ
        {{ check_qmj_column('safety') }} AS unqualified_reason
    FROM calc_evol
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    bab_score,
    lev_score,
    ohlson_o_score,
    altman_z_score,
    evol_score,
    
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason

    -- Audit Columns
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM apply_dq