



-- STEP 1: Extract qualified recent scores for Value and Momentum
with
    base_value as (
        select ticker, value_recent_score, last_market_cap_update
        from "lakehouse_main"."intermediate"."int_value_ratio_recent"
        where status = 'qualified'
    ),

    base_momentum as (
        select ticker, momentum_recent, last_trade_date
        from "lakehouse_main"."intermediate"."int_momentum_ratio_recent"
        where status = 'qualified'
    ),

    -- STEP 2: Combine metrics (Full Outer Join to retain all valid tickers)
    joined_metrics as (
        select ticker, v.value_recent_score, m.momentum_recent
        from base_value v
        full outer join base_momentum m using (ticker)
    ),

    -- STEP 3: Rank metrics cross-sectionally across the entire current market
    -- Note: No partition clause since this is a live snapshot of the whole market
    ranked_metrics as (
        select
            *,
            case
                when value_recent_score is not NULL
                then RANK() over (order by value_recent_score ASC)
                else NULL
            end as value_rank,

            case
                when momentum_recent is not NULL
                then RANK() over (order by momentum_recent ASC)
                else NULL
            end as momentum_rank
        from joined_metrics
    ),
    -- STEP 4: Standardize ranks into Z-Scores
    z_score_components as (
        select
            *,
            -- Z-Score for Live Value
            (value_rank - AVG(value_rank) over ())
            / NULLIF(STDDEV_SAMP(value_rank) over (), 0) as z_value_recent,

            -- Z-Score for Live Momentum
            (momentum_rank - AVG(momentum_rank) over ())
            / NULLIF(STDDEV_SAMP(momentum_rank) over (), 0) as z_momentum_recent

        from ranked_metrics
    ),

    -- STEP 5: Apply inline Data Quality Rules
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case
                        when z_value_recent is NULL
                        then 'z_value_recent is null'
                        else NULL
                    end,
                    case
                        when z_momentum_recent is NULL
                        then 'z_momentum_recent is null'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from z_score_components
    )

-- STEP 6: Final Selection, Status Resolution, and Audit Injection
select
    ticker,
    value_recent_score,
    momentum_recent,
    z_value_recent,
    z_momentum_recent,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as gold_updated_at , '4ff423e7-7675-4eec-a090-58bdf9560b12' as gold_invocation_id 

from applied_dq_rules