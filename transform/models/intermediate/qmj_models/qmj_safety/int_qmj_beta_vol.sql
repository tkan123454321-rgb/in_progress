{{ config(
    materialized='ephemeral',
    tags=['intermediate', 'qmj_beta_vol']
) }}


-- STEP 1: Extract qualified daily stock prices
WITH silver_stock AS (
    SELECT 
        date, 
        ticker, 
        price_close, 
        price_basic
    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' 
        AND ticker != 'VNINDEX'
),

-- STEP 2: Extract qualified daily market index (VNINDEX) prices
silver_vnindex AS (
    SELECT 
        date, 
        price_close, 
        price_basic
    FROM {{ ref('silver_historical_quotes') }}
    WHERE status = 'qualified' AND ticker = 'VNINDEX'
),

-- STEP 3: Calculate daily logarithmic returns for both stock and market
joined_daily_returns AS (
    SELECT 
        s.date,
        s.ticker,

        LN(s.price_close / NULLIF(s.price_basic, 0)) AS stock_ret,
        LN(m.price_close / NULLIF(m.price_basic, 0)) AS mkt_ret

    FROM silver_stock s
    JOIN silver_vnindex m 
        ON s.date = m.date
),

-- STEP 4: Calculate 1-year rolling volatilities (252 trading days)
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

-- STEP 5: Apply Data Quality Rules for volatility calculations
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. Check if Log Return calculation resulted in NULL due to division by zero
                CASE WHEN stock_ret IS NULL THEN 'stock_ret (log return) is null' ELSE NULL END,
                CASE WHEN mkt_ret IS NULL THEN 'mkt_ret (market log return) is null' ELSE NULL END,
                
                -- 2. Apply AQR's rule: Minimum of 120 trading days required
                CASE WHEN count_trading_days < 120 THEN 'Err: Not enough trading days (<120)' ELSE NULL END,
                
                -- 3. Check final Standard Deviation results
                CASE WHEN vol_stock_1y IS NULL THEN 'vol_stock_1y is null' ELSE NULL END,
                CASE WHEN vol_mkt_1y IS NULL THEN 'vol_mkt_1y is null' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM rolling_volatilities
)

-- STEP 6: Finalize ephemeral model with status
SELECT 
    date,
    ticker,
    stock_ret,
    mkt_ret,
    vol_stock_1y,
    vol_mkt_1y,
    count_trading_days,
    unqualified_reason,
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status
    -- Note: Audit columns are omitted as this is an ephemeral model
FROM applied_dq_rules