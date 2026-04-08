{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_beta_final'],
    order_by=['ticker', 'date'] 
) }}

WITH daily_data AS (
    SELECT * FROM {{ ref('int_qmj_beta_vol') }} -- Trỏ vào bảng vol bác vừa làm xong
    WHERE status = 'qualified'
),
three_day_calculation AS (
    -- Bước 1: Tính Lợi nhuận gộp 3 ngày (Overlapping)
    SELECT 
        *,
        SUM(stock_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_ret_3d,
        SUM(mkt_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mkt_ret_3d
    FROM daily_data
),

correlation_calculation AS (
    -- Bước 2: Tính Tương quan 4 năm (1008 ngày giao dịch)
    SELECT 
        *,
        -- 4 năm x 252 ngày = 1008 ngày. Lùi 1007 dòng + dòng hiện tại
        CORR(stock_ret_3d, mkt_ret_3d) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 1007 PRECEDING AND CURRENT ROW
        ) AS rho_4y,
        
        -- Đếm số ngày thực tế để check luật tối thiểu 3 năm (750 ngày)
        COUNT(stock_ret_3d) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 1007 PRECEDING AND CURRENT ROW
        ) AS count_corr_days
    FROM three_day_calculation
),

final_beta_logic AS (
    -- Bước 3: Ráp công thức Beta và Shrinkage
    SELECT 
        *,
        rho_4y * (vol_stock_1y / NULLIF(vol_mkt_1y, 0)) AS beta_ts
    FROM correlation_calculation
),

applied_dq_rules AS (
    SELECT 
        *,
        -- Gọi Macro DQ cho bước tính Beta
        {{ check_qmj_column('beta_final_calculation') }} AS unqualified_reason
    FROM final_beta_logic
)

SELECT 
    ticker,
    date,
    vol_stock_1y,
    vol_mkt_1y,
    stock_ret_3d,
    mkt_ret_3d,
    rho_4y,
    count_corr_days,
    beta_ts,
    (0.6 * beta_ts + (0.4 * 1.0)) AS beta_final,

    -- Data Quality columns
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

FROM applied_dq_rules
