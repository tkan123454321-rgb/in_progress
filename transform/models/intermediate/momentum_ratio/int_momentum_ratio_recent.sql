{{ config(materialized="table", tags=["intermediate", "recent_momentum"]) }}

{% set audit_cols = get_audit_columns("intermediate") %}

-- STEP 1: Extract daily prices and calculate 1-month (21 days) and 1-year (252 days)
-- lags
with
    daily_quotes_lagged as (
        select
            ticker,
            date,
            price_basic as close_price_adj,

            -- Price and Date from 1 month ago (~21 trading days)
            LAG(price_basic, 21) over w_ticker as price_t_21,
            LAG(date, 21) over w_ticker as date_t_21,

            -- Price and Date from 1 year ago (~252 trading days)
            LAG(price_basic, 252) over w_ticker as price_t_252,
            LAG(date, 252) over w_ticker as date_t_252

        from {{ ref("silver_historical_quotes") }}
        where status = 'qualified' and ticker != 'VNINDEX'
        window w_ticker as (partition by ticker order by date ASC)
    ),

    -- STEP 2: Calculate 12M-1M Momentum and check data freshness
    calc_recent_momentum as (
        select
            *,
            -- AQR's 12M-1M Momentum: (Price at T-1 month / Price at T-12 months) - 1
            (price_t_21 / NULLIF(price_t_252, 0)) - 1 as momentum_recent,

            -- Calculate days since last trade for freshness check
            DATE_DIFF('day', CAST(date as DATE), CURRENT_DATE) as days_since_last_trade
        from daily_quotes_lagged
    ),

    -- STEP 3: Filter for the most recent trading day per ticker
    latest_state as (
        select *, ROW_NUMBER() over (partition by ticker order by date DESC) as rn
        from calc_recent_momentum
    ),

    -- STEP 4: Apply inline Data Quality Rules specific to Recent Momentum
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- 1. Freshness Check: Suspended or no trades for > 10 days
                    case
                        when days_since_last_trade > 10
                        then 'Err: Stale Data (Suspended or no trades > 10 days)'
                        else NULL
                    end,

                    -- 2. Ensure 1-month benchmark exists
                    case
                        when price_t_21 is NULL or price_t_21 = 0
                        then 'Err: Missing 1-month benchmark (price_t_21 is null or 0)'
                        else NULL
                    end,

                    -- 3. Ensure 1-year history exists
                    case
                        when price_t_252 is NULL or price_t_252 = 0
                        then
                            'Err: Insufficient 1-year history (price_t_252 is null or 0)'
                        else NULL
                    end,

                    -- 4. Final validation
                    case
                        when momentum_recent is NULL
                        then
                            'Err: Momentum calculation failed (momentum_recent is null)'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from latest_state
        where rn = 1
    )

-- STEP 5: Final Selection and Status Resolution
select
    ticker,
    date as last_trade_date,
    days_since_last_trade,
    date_t_21,
    price_t_21,
    date_t_252,
    price_t_252,
    momentum_recent,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules
