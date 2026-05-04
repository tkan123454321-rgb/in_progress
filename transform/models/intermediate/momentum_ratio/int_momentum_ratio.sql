{{ config(materialized="table", tags=["intermediate", "historical_momentum"]) }}

{% set audit_cols = get_audit_columns("intermediate") %}
-- STEP 1: Extract End-of-Month (EOM) prices for stocks only
with
    eom_prices as (
        select
            ticker,
            date,
            year,
            quarter,
            month,
            absolute_month,
            absolute_quarter,
            price_basic as close_price_adj,
            ROW_NUMBER() over (
                partition by ticker, absolute_month order by date DESC
            ) as rn
        from {{ ref("silver_historical_quotes") }}
        where status = 'qualified' and ticker != 'VNINDEX'
    ),
    -- STEP 2: Filter to keep only the last trading day of each month
    monthly_series as (select * from eom_prices where rn = 1),
    -- STEP 3: Create 1-month and 12-month lagged price points
    lagged_prices as (
        select
            ticker,
            year,
            quarter,
            month,
            absolute_month,
            absolute_quarter,
            close_price_adj,

            -- Price from 1 month ago (T-1)
            LAG(close_price_adj, 1) over w_ticker as price_t_1,

            -- Price from 12 months ago (T-12)
            LAG(close_price_adj, 12) over w_ticker as price_t_12,

            -- Gap check to enforce strict 12-month historical availability
            (absolute_month - LAG(absolute_month, 12) over w_ticker) as gap_12m

        from monthly_series
        window w_ticker as (partition by ticker order by absolute_month)
    ),

    -- STEP 4: Calculate the raw 12M-1M Momentum score
    calc_raw_momentum as (
        select *, (price_t_1 / NULLIF(price_t_12, 0)) - 1 as momentum_raw_score
        from lagged_prices
    ),

    -- STEP 5: Align monthly momentum scores to quarter-ends
    quarterly_alignment as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, year, quarter order by month DESC
            ) as rn_q
        from calc_raw_momentum
    ),

    -- STEP 6: Apply inline Data Quality Rules specific to Historical Momentum
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Strict 12-month gap check
                    case
                        when gap_12m is NULL or gap_12m != 12
                        then 'Err: Missing 12-month lag for Price (Gap != 12)'
                        else NULL
                    end,

                    -- Null or zero checks for lagged prices
                    case
                        when price_t_1 is NULL
                        then 'Err: Lagged Price 1m is null'
                        else NULL
                    end,
                    case
                        when price_t_12 is NULL or price_t_12 = 0
                        then 'Err: Lagged Price 12m is null or zero'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from quarterly_alignment
        where rn_q = 1
    )
-- STEP 7: Final Selection and Status Resolution
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    month as end_of_quarter_month,
    absolute_month,
    gap_12m,
    price_t_1,
    price_t_12,
    momentum_raw_score,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules
