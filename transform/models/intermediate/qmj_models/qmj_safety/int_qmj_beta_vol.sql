{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_beta_vol'],
    properties={
        "sorted_by": "ARRAY['ticker', 'date']"
    }
) }}

WITH silver_stock AS (
    SELECT date, ticker, price_close, price_basic
    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker != 'VNINDEX'
),

silver_vnindex AS (
    SELECT date, price_close, price_basic
    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker = 'VNINDEX'
),

joined_daily_returns AS (
    SELECT 
        s.date,
        s.ticker,
        
        -- Sạch sẽ tuyệt đối: Tin tưởng 100% vào Data Type từ lớp Silver
        LN(s.price_close / NULLIF(s.price_basic, 0)) AS stock_ret,
        LN(m.price_close / NULLIF(m.price_basic, 0)) AS mkt_ret

    FROM silver_stock s
    JOIN silver_vnindex m ON s.date = m.date
),

rolling_volatilities AS (
    SELECT 
        date,
        ticker,
        stock_ret,
        mkt_ret,
        STDDEV_SAMP(stock_ret) OVER w_1y AS vol_stock_1y,
        STDDEV_SAMP(mkt_ret) OVER w_1y AS vol_mkt_1y,
        COUNT(stock_ret) OVER w_1y AS count_trading_days
    FROM joined_daily_returns
    WINDOW w_1y AS (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW 
    )
),

applied_dq_rules AS (
    SELECT 
        *,
        {{ check_qmj_column('beta_volatility') }} AS unqualified_reason
    FROM rolling_volatilities
)

SELECT 
    date,
    ticker,
    stock_ret,
    mkt_ret,
    vol_stock_1y,
    vol_mkt_1y,
    count_trading_days,
    
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_dq_rules