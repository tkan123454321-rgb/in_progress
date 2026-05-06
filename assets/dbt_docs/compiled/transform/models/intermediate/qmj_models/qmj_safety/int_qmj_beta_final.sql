-- STEP 1: Extract qualified volatility data and remove ephemeral DQ columns
with
    __dbt__cte__int_qmj_beta_vol as (

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
                            -- 1. Check if Log Return calculation resulted in NULL due
                            -- to
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
                                when vol_stock_1y is NULL
                                then 'vol_stock_1y is null'
                                else NULL
                            end,
                            case
                                when vol_mkt_1y is NULL
                                then 'vol_mkt_1y is null'
                                else NULL
                            end
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
    ),
    daily_data_raw as (
        select * from __dbt__cte__int_qmj_beta_vol where status = 'qualified'
    ),

    daily_data_cleaned as (
        -- Exclude inherited status and reason columns to avoid conflicts
        select *
        from
            TABLE(
                exclude_columns(
                    input => TABLE(daily_data_raw),
                    columns => DESCRIPTOR(status, unqualified_reason)
                )
            )
    ),

    -- STEP 2: Calculate 3-day overlapping returns
    three_day_calculation as (
        select
            *,
            SUM(stock_ret) over (
                partition by ticker
                order by date
                rows between 2 PRECEDING and CURRENT ROW
            ) as stock_ret_3d,
            SUM(mkt_ret) over (
                partition by ticker
                order by date
                rows between 2 PRECEDING and CURRENT ROW
            ) as mkt_ret_3d
        from daily_data_cleaned
    ),

    -- STEP 3: Calculate 4-year rolling correlation (1008 trading days)
    correlation_calculation as (
        select
            *,
            -- Calculate Pearson correlation coefficient over 4 years
            CORR(stock_ret_3d, mkt_ret_3d) over w_4y as rho_4y,

            -- Count actual data points to enforce the 3-year minimum data rule
            COUNT(stock_ret_3d) over w_4y as count_corr_days
        from three_day_calculation
        window
            w_4y as (
                partition by ticker
                order by date
                rows between 1007 PRECEDING and CURRENT ROW
            )
    ),

    -- STEP 4: Calculate Time-Series Beta
    time_series_beta as (
        select
            *,
            -- Beta = Correlation * (Stock Volatility / Market Volatility)
            rho_4y * (vol_stock_1y / NULLIF(vol_mkt_1y, 0)) as beta_ts
        from correlation_calculation
    ),

    -- STEP 5: Calculate final metrics, aggregate to quarter-end, and rank rows
    quarter_aggregation as (
        select
            ticker,
            date,
            EXTRACT(YEAR from date) as year,
            EXTRACT(QUARTER from date) as quarter,
            (EXTRACT(YEAR from date) * 4)
            + EXTRACT(QUARTER from date) as absolute_quarter,

            -- Underlying Metrics
            vol_stock_1y,
            vol_mkt_1y,
            stock_ret_3d,
            mkt_ret_3d,
            rho_4y,
            count_corr_days,
            beta_ts,

            -- Shrinkage Beta: Vasicek's bayesian shrinkage toward 1.0
            (0.6 * beta_ts + 0.4) as beta_final,

            -- Betting Against Beta (BAB) Score
            -1 * ((0.6 * beta_ts) + 0.4) as bab_score,

            -- Rank to extract the last trading day of each quarter
            ROW_NUMBER() over (
                partition by ticker, EXTRACT(YEAR from date), EXTRACT(QUARTER from date)
                order by date DESC
            ) as rn
        from time_series_beta
    ),
    -- STEP 6: Filter Quarter-End records and Apply Data Quality Rules
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Require at least ~3 years of data (750 days) for a valid
                    -- correlation
                    case
                        when count_corr_days < 750
                        then 'Err: Not enough data for correlation (<750 days)'
                        else NULL
                    end,
                    -- Check for null beta (usually caused by zero market volatility)
                    case
                        when beta_ts is NULL
                        then 'beta_ts is null (check vol_mkt_1y)'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from quarter_aggregation
        where rn = 1
    )

-- STEP 7: Final Selection (Resolve final status and inject metadata)
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    vol_stock_1y,
    vol_mkt_1y,
    stock_ret_3d,
    mkt_ret_3d,
    rho_4y,
    count_corr_days,
    beta_ts,
    beta_final,
    bab_score,

    -- Resolve Final Status Here
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason,
    -- Auto-generated audit columns
    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at,
    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as int_invocation_id

from applied_dq_rules
