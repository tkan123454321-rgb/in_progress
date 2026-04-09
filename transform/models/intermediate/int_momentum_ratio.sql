{{ config(
    materialized='table',
    tags=['intermediate', 'momentum_scoring']
) }}

WITH eom_prices AS (
    -- 1. LẤY NGÀY GIAO DỊCH CUỐI CÙNG CỦA TỪNG THÁNG
    SELECT 
        ticker,
        date,
        year,
        quarter,
        month,
        absolute_month,
        absolute_quarter,
        price_basic AS close_price_adj,
        
        ROW_NUMBER() OVER (
            PARTITION BY ticker, absolute_month 
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker != 'VNINDEX' -- Loại bỏ chỉ số chung, chỉ tập trung vào cổ phiếu
),

monthly_series AS (
    -- 2. LỌC RA CHUỖI GIÁ THÁNG
    SELECT * FROM eom_prices 
    WHERE rn = 1
),

lagged_prices AS (
    -- 3. CHUẨN BỊ ĐỂ TÍNH MOMENTUM (12-1)
    SELECT 
        ticker,
        year,
        quarter,
        month,
        absolute_month,
        absolute_quarter,
        close_price_adj,
        
        -- Lấy giá của 1 tháng trước (T-1)
        LAG(close_price_adj, 1) OVER (PARTITION BY ticker ORDER BY absolute_month) AS price_t_1,
        
        -- Lấy giá của 12 tháng trước (T-12)
        LAG(close_price_adj, 12) OVER (PARTITION BY ticker ORDER BY absolute_month) AS price_t_12,
        
        -- Chốt chặn Gap 12
        (absolute_month - LAG(absolute_month, 12) OVER (PARTITION BY ticker ORDER BY absolute_month)) AS gap_12m

    FROM monthly_series
),

calc_momentum AS (
    -- 4. TÍNH ĐIỂM ĐỘNG LƯỢNG THÔ THEO THÁNG
    SELECT 
        *,
        (price_t_1 / NULLIF(price_t_12, 0)) - 1 AS momentum_raw_score
    FROM lagged_prices
),

quarterly_alignment AS (
    -- 5. CHỐT SỔ THEO QUÝ (Ý tưởng tuyệt vời của bác)
    SELECT 
        *,
        -- Xếp hạng tháng giảm dần trong từng quý để lấy tháng cuối cùng có dữ liệu
        ROW_NUMBER() OVER (
            PARTITION BY ticker, year, quarter 
            ORDER BY month DESC
        ) AS rn_q
    FROM calc_momentum
)

SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    month AS end_of_quarter_month, -- Đổi tên chút cho rõ nghĩa đây là tháng đại diện quý
    absolute_month,
    gap_12m,
    price_t_1,
    price_t_12,
    momentum_raw_score,

    -- 6. BẮT LỖI BẰNG MACRO
    {{ check_value_and_momentum_column('momentum') }} AS unqualified_reason,
    CASE 
        WHEN {{ check_value_and_momentum_column('momentum') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Audit columns
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM quarterly_alignment
WHERE rn_q = 1 -- Lưới lọc cuối cùng: Chỉ cho phép 1 dòng / 1 quý đi tiếp