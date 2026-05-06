



-- STEP 1: Extract qualified Z-scores from the intermediate growth layer
with
    intermediate_data as (
        select * from "lakehouse_main"."intermediate"."int_qmj_scoring_growth" where status = 'qualified'
    ),

    -- STEP 2: Sum the 5 standardized growth components
    calculate_sum as (
        select
            *,
            (
                z_delta_gpoa + z_delta_roe + z_delta_roa + z_delta_cfoa + z_delta_gmar
            ) as raw_growth_sum
        from intermediate_data
    ),

    -- STEP 3: Rank the aggregated growth sums cross-sectionally per quarter
    final_ranking as (
        select
            *,
            RANK() over (
                partition by absolute_quarter order by raw_growth_sum ASC
            ) as final_rank
        from calculate_sum
    )

-- STEP 4: Final Selection and Calculation of the AQR Growth Z-Score
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    -- Retain underlying components for downstream dashboards and debugging
    z_delta_gpoa,
    z_delta_roe,
    z_delta_roa,
    z_delta_cfoa,
    z_delta_gmar,

    -- Final AQR Growth Score: Z-Score of the cross-sectional ranks
    (final_rank - AVG(final_rank) over w_qtr)
    / NULLIF(STDDEV_SAMP(final_rank) over w_qtr, 0) as qmj_growth_score

    -- Auto-generated audit columns for the Gold layer
    , CAST(from_iso8601_timestamp('2026-05-06T08:55:22.931753+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as gold_updated_at , '273468de-8a49-4a91-9bc2-2aabb801915e' as gold_invocation_id 

from final_ranking
window w_qtr as (partition by absolute_quarter)