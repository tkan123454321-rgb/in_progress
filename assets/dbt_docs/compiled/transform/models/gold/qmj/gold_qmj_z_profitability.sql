



-- STEP 1: Extract qualified Z-scores from the intermediate profitability layer
with
    intermediate_data as (
        select *
        from "lakehouse_main"."intermediate"."int_qmj_scoring_profitability"
        where status = 'qualified'
    ),

    -- STEP 2: Sum the 6 standardized profitability components
    calculate_sum as (
        select
            *,
            (z_gpoa + z_roe + z_roa + z_cfoa + z_gmar + z_acc) as raw_profitability_sum
        from intermediate_data
    ),

    -- STEP 3: Rank the aggregated profitability sums cross-sectionally per quarter
    final_ranking as (
        select
            *,
            RANK() over (
                partition by absolute_quarter order by raw_profitability_sum ASC
            ) as final_rank
        from calculate_sum
    )

-- STEP 4: Final Selection and Calculation of the AQR Profitability Z-Score
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    -- Retain underlying components for downstream dashboards and debugging
    z_gpoa,
    z_roe,
    z_roa,
    z_cfoa,
    z_gmar,
    z_acc,

    -- Final AQR Profitability Score: Z-Score of the cross-sectional ranks
    (final_rank - AVG(final_rank) over w_qtr)
    / NULLIF(STDDEV_SAMP(final_rank) over w_qtr, 0) as qmj_profitability_score

    -- Auto-generated audit columns for the Gold layer
    , CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as gold_updated_at , '4ff423e7-7675-4eec-a090-58bdf9560b12' as gold_invocation_id 

from final_ranking
window w_qtr as (partition by absolute_quarter)