



-- STEP 1: Extract current market capitalization and freshness from Gold layer
with
    gold_market_cap as (
        select
            ticker,
            market_cap,
            gold_updated_at,
            DATE_DIFF(
                'day', CAST(gold_updated_at as DATE), CURRENT_DATE
            ) as days_since_update
        from "lakehouse_main"."gold"."gold_dim_company"
        where ticker != 'VNINDEX'
    ),

    -- STEP 2: Extract the most recent fundamental metrics (Book Equity) per ticker
    latest_fundamentals as (
        select
            ticker,
            year as report_year,
            quarter as report_quarter,
            absolute_quarter as latest_absolute_quarter,

            -- Core Book Equity
            (total_equity - minority_interest - preferred_stock) as latest_book_equity,

            ROW_NUMBER() over (
                partition by ticker order by absolute_quarter DESC
            ) as rn_q

        from "lakehouse_main"."intermediate"."int_ttm_metrics"
        where ttm_status = 'valid_ttm'
    ),

    -- STEP 3: Combine market data with fundamentals and calculate reporting delays
    live_value_calculation as (
        select
            c.ticker,
            c.gold_updated_at as last_market_cap_update,
            c.days_since_update,
            c.market_cap,

            f.report_year,
            f.report_quarter,
            f.latest_absolute_quarter,
            f.latest_book_equity,

            -- Calculate current absolute quarter based on system date
            (
                EXTRACT(YEAR from CURRENT_DATE) * 4 + EXTRACT(QUARTER from CURRENT_DATE)
            ) as current_absolute_quarter,

            -- Calculate delay in quarters
            (
                (
                    EXTRACT(YEAR from CURRENT_DATE) * 4
                    + EXTRACT(QUARTER from CURRENT_DATE)
                )
                - f.latest_absolute_quarter
            ) as quarters_delayed,

            -- Value Score (Book Equity / Market Cap)
            (f.latest_book_equity / NULLIF(c.market_cap, 0)) as value_recent_score

        from gold_market_cap c
        inner join latest_fundamentals f on c.ticker = f.ticker and f.rn_q = 1
    ),

    -- STEP 4: Apply inline Data Quality Rules specific to Recent Value
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- 1. Freshness filter for Market Cap (> 10 days = unqualified)
                    case
                        when days_since_update > 10
                        then 'Err: Stale Data in gold_dim_company (> 10 days)'
                        else NULL
                    end,

                    -- 2. Freshness filter for Fundamentals (> 2 quarters delayed =
                    -- unqualified)
                    case
                        when quarters_delayed > 2
                        then 'Err: Stale Fundamental Data (Delayed > 2 Quarters)'
                        else NULL
                    end,

                    -- 3. Validation for calculated score
                    case
                        when value_recent_score is NULL
                        then 'Err: Invalid Value Recent Score'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from live_value_calculation
    )

-- STEP 5: Final Selection and Status Resolution
select
    ticker,
    last_market_cap_update,
    days_since_update,
    report_year,
    report_quarter,
    latest_absolute_quarter,
    current_absolute_quarter,
    quarters_delayed,
    market_cap,
    latest_book_equity,
    value_recent_score,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:55:22.931753+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , '273468de-8a49-4a91-9bc2-2aabb801915e' as int_invocation_id 

from applied_dq_rules