{{ config(materialized='table', tags=['intermediate', 'qmj', 'safety']) }}


WITH base_metrics AS (
    SELECT 
        *,
        (total_equity - minority_interest - preferred_stock) AS book_equity
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),

prep_lags AS (
    SELECT 
        *,
        -- Lấy Lợi nhuận ròng của 1 năm trước (4 quý)
        LAG(net_income_ttm, 4) OVER w AS net_income_past,
        
        -- Check độ liền mạch dữ liệu (Quarter Gap)
        (absolute_quarter - LAG(absolute_quarter, 4) OVER w) AS quarter_gap_4
        
    FROM base_metrics
    WINDOW w AS (PARTITION BY ticker ORDER BY absolute_quarter)
),

calc_components AS (
    SELECT 
        *,
        -- ADJASSET: AT + 0.1 * (ME - BE)
        (total_assets + 0.1 * (market_cap - book_equity)) AS adj_asset,
        
        -- Tổng nợ = Nợ ngắn hạn + Nợ dài hạn
        (short_term_debt + long_term_debt) AS total_debt
    FROM prep_lags
),

calc_ohlson_vars AS (
    SELECT
        *,
        -- 9 biến số cốt lõi của Ohlson
        LN(NULLIF((total_assets + 0.1 * (market_cap - book_equity)), 0) / NULLIF(cpi_index, 0)) AS log_size,
        (total_debt / NULLIF(adj_asset, 0)) AS tlta,
        ((current_assets - current_liabilities) / NULLIF(adj_asset, 0)) AS wcta,
        (current_liabilities / NULLIF(current_assets, 0)) AS clca,
        CASE WHEN total_liabilities > total_assets THEN 1 ELSE 0 END AS oeneg,
        (net_income_ttm / NULLIF(total_assets, 0)) AS nita,
        (profit_before_tax_ttm / NULLIF(total_liabilities, 0)) AS futl,
        CASE WHEN GREATEST(net_income_ttm, net_income_past) < 0 THEN 1 ELSE 0 END AS intwo,
        (net_income_ttm - net_income_past) / NULLIF(ABS(net_income_ttm) + ABS(net_income_past), 0) AS chin
    FROM calc_components
),

calc_o_score AS (
    SELECT
        *,
        -- Giờ công thức cuối cùng cực kỳ thanh thoát
        -(
            -1.32 
            - 0.407 * log_size -- Đã tính ở tầng trên
            + 6.03 * tlta
            - 1.43 * wcta
            + 0.076 * clca
            - 1.72 * oeneg
            - 2.37 * nita
            - 1.83 * futl
            + 0.285 * intwo
            - 0.521 * chin
        ) AS ohlson_o_score
    FROM calc_ohlson_vars
),

apply_dq AS (
    SELECT 
        *,
        {{ check_qmj_column('o_score_safety') }} AS unqualified_reason
    FROM calc_o_score
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    log_size,
    tlta,
    wcta,
    clca,
    oeneg,
    nita,
    futl,
    intwo,
    chin,
    ohlson_o_score,
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason
    
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
from apply_dq