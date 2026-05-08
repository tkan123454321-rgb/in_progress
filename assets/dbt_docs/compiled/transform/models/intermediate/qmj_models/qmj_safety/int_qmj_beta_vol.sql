


-- STEP 1: Extract qualified daily stock prices
with
    silver_stock as (
        select date, ticker, price_close, price_basic
        from "lakehouse_main"."silver"."silver_historical_quotes"
        where status = 'qualified' and ticker != 'VNINDEX'
    ),

    -- STEP 2: Extract qualified daily market index (VNINDEX) prices
    silver_vnindex as (
        select date, price_close, price_basic
        from "lakehouse_main"."silver"."silver_historical_quotes"
        where status = 'qualified' and ticker = 'VNINDEX'
    ),

    -- STEP 3: Calculate daily logarithmic returns for both stock and market
    joined_daily_returns as (
        select
            s.date,
            s.ticker,

            LN(s.price_close / NULLIF(s.price_basic, 0)) as stock_ret,
            LN(m.price_close / NULLIF(m.price_basic, 0)) as mkt_ret

        from silver_stock s
        join silver_vnindex m on s.date = m.date
    ),

    -- STEP 4: Calculate 1-year rolling volatilities (252 trading days)
    rolling_volatilities as (
        select
            date,
            ticker,
            stock_ret,
            mkt_ret,
            STDDEV_SAMP(stock_ret) over w_1y as vol_stock_1y,
            STDDEV_SAMP(mkt_ret) over w_1y as vol_mkt_1y,
            COUNT(stock_ret) over w_1y as count_trading_days
        from joined_daily_returns
        window
            w_1y as (
                partition by ticker
                order by date
                rows between 251 PRECEDING and CURRENT ROW
            )
    ),

    -- STEP 5: Apply Data Quality Rules for volatility calculations
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- 1. Check if Log Return calculation resulted in NULL due to
                    -- division by zero
                    case
                        when stock_ret is NULL
                        then 'stock_ret (log return) is null'
                        else NULL
                    end,
                    case
                        when mkt_ret is NULL
                        then 'mkt_ret (market log return) is null'
                        else NULL
                    end,

                    -- 2. Apply AQR's rule: Minimum of 120 trading days required
                    case
                        when count_trading_days < 120
                        then 'Err: Not enough trading days (<120)'
                        else NULL
                    end,

                    -- 3. Check final Standard Deviation results
                    case
                        when vol_stock_1y is NULL then 'vol_stock_1y is null' else NULL
                    end,
                    case when vol_mkt_1y is NULL then 'vol_mkt_1y is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from rolling_volatilities
    )

-- STEP 6: Finalize ephemeral model with status
select
    date,
    ticker,
    stock_ret,
    mkt_ret,
    vol_stock_1y,
    vol_mkt_1y,
    count_trading_days,
    unqualified_reason,
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status
-- Note: Audit columns are omitted as this is an ephemeral model
from applied_dq_rules