{{ config(
    materialized='table',
    tags=['intermediate', 'daily_momentum_scoring']
) }}

WITH daily_quotes AS (
    -- 1. LẤY DỮ LIỆU GIÁ VÀ TRÍCH XUẤT CÁC ĐIỂM LÙI (LAG)
    SELECT 
        ticker,
        date,
        price_basic AS close_price_adj,
        
        -- Lấy GIÁ và NGÀY của 21 phiên trước (~1 tháng)
        LAG(price_basic, 21) OVER (PARTITION BY ticker ORDER BY date ASC) AS price_t_21,
        LAG(date, 21) OVER (PARTITION BY ticker ORDER BY date ASC) AS date_t_21,
        
        -- Lấy GIÁ và NGÀY của 252 phiên trước (~1 năm)
        LAG(price_basic, 252) OVER (PARTITION BY ticker ORDER BY date ASC) AS price_t_252,
        LAG(date, 252) OVER (PARTITION BY ticker ORDER BY date ASC) AS date_t_252

    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker != 'VNINDEX'
),

calc_momentum AS (
    -- 2. TÍNH MOMENTUM VÀ XÁC ĐỊNH ĐỘ TRỄ DỮ LIỆU
    SELECT 
        *,
        -- Công thức 12M-1M: (P_t-21 / P_t-252) - 1
        (price_t_21 / NULLIF(price_t_252, 0)) - 1 AS momentum_recent,
        
        -- Tính khoảng cách ngày so với hôm nay
        DATE_DIFF('day', date, CURRENT_DATE) AS days_since_last_trade
    FROM daily_quotes
),

latest_state AS (
    -- 3. CHỈ LẤY PHIÊN GẦN NHẤT CỦA MỖI MÃ
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
    FROM calc_momentum
)

SELECT 
    ticker,
    date AS last_trade_date,
    days_since_last_trade,
    
    -- XUẤT CÁC CỘT ĐỐI CHIẾU RA ĐỂ DỄ SOI
    date_t_21,
    price_t_21,
    date_t_252,
    price_t_252,
    
    momentum_recent,
    
   -- GỌI MACRO BẮT LỖI
   {{ check_value_and_momentum_column('momentum_recent') }} AS unqualified_reason,

    -- CHỐT STATUS GỌN GÀNG
    CASE 
        WHEN {{ check_value_and_momentum_column('momentum_recent') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Audit columns
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM latest_state
WHERE rn = 1