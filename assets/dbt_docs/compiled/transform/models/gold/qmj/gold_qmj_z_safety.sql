-- STEP 1: Extract qualified Z-scores from the intermediate safety layer
with
    intermediate_data as (
        select *
        from "lakehouse_main"."intermediate"."int_qmj_scoring_safety"
        where status = 'qualified'
    ),

    -- STEP 2: Sum the 5 standardized safety components
    calculate_sum as (
        select *, (z_bab + z_lev + z_o + z_z + z_evol) as raw_safety_sum
        from intermediate_data
    ),

    -- STEP 3: Rank the aggregated safety sums cross-sectionally per quarter
    final_ranking as (
        select
            *,
            RANK() over (
                partition by absolute_quarter order by raw_safety_sum ASC
            ) as final_rank
        from calculate_sum
    )

-- STEP 4: Final Selection and Calculation of the AQR Safety Z-Score
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    -- Retain underlying components for downstream dashboards and debugging
    z_bab,
    z_lev,
    z_o,
    z_z,
    z_evol,

    -- Final AQR Safety Score: Z-Score of the cross-sectional ranks
    (final_rank - AVG(final_rank) over w_qtr)
    / NULLIF(STDDEV_SAMP(final_rank) over w_qtr, 0) as qmj_safety_score,
    -- Auto-generated audit columns for the Gold layer
    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as gold_updated_at,
    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as gold_invocation_id

from final_ranking
window w_qtr as (partition by absolute_quarter)
