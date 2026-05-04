{{ config(materialized="table", tags=["gold", "qmj_safety"]) }}

{% set audit_cols = get_audit_columns("gold") %}

-- STEP 1: Extract qualified Z-scores from the intermediate safety layer
with
    intermediate_data as (
        select * from {{ ref("int_qmj_scoring_safety") }} where status = 'qualified'
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
    / NULLIF(STDDEV_SAMP(final_rank) over w_qtr, 0) as qmj_safety_score

    -- Auto-generated audit columns for the Gold layer
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from final_ranking
window w_qtr as (partition by absolute_quarter)
