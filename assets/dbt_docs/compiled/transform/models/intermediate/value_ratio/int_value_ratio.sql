


-- STEP 1: Extract fundamentals and calculate 6-month (2 quarters) lagged Book Equity
with
    historical_lags as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,

            -- Historical Market Cap (T)
            market_cap,

            -- Standard Book Equity (T - 6 months)
            LAG(total_equity - minority_interest - preferred_stock, 2) over w_ticker
            as book_equity_6m_lag,

            -- Gap check to ensure the lag is exactly 2 quarters
            (absolute_quarter - LAG(absolute_quarter, 2) over w_ticker) as gap_6m

        from "lakehouse_main"."intermediate"."int_ttm_metrics"
        where ttm_status = 'valid_ttm'
        window w_ticker as (partition by ticker order by absolute_quarter)
    ),

    -- STEP 2: Calculate the Raw Value Score
    raw_value_calculation as (
        select
            *,
            -- Value Score = Book Equity (t-2) / Market Equity (t)
            (book_equity_6m_lag / NULLIF(market_cap, 0)) as value_raw_score
        from historical_lags
    ),

    -- STEP 3: Apply Data Quality Rules specific to Value
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Continuity Check: Must have exactly a 2-quarter gap
                    case
                        when gap_6m is NULL or gap_6m != 2
                        then 'Err: Missing 6-month lag for Book Equity (Gap != 2)'
                        else NULL
                    end,

                    -- Null check for historical book equity
                    case
                        when book_equity_6m_lag is NULL
                        then 'Err: Lagged Book Equity is null'
                        else NULL
                    end,

                    -- Null check for value_raw_score
                    case
                        when value_raw_score is NULL
                        then 'Err: Value Score is null'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from raw_value_calculation
    )
-- STEP 4: Final Selection and Status Resolution
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    gap_6m,
    market_cap,
    book_equity_6m_lag,
    value_raw_score,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:48:04.916793+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , 'd5f144b3-ec78-4c38-93a0-f54d53bb219b' as int_invocation_id 

from applied_dq_rules