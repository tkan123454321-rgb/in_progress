



-- STEP 1: Extract qualified raw growth metrics
with
    base_metrics as (
        select * from "lakehouse_main"."intermediate"."int_qmj_growth" where status = 'qualified'
    ),

    -- STEP 2: Rank each growth component cross-sectionally per quarter
    ranked_growth as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,

            -- Ranks (Ascending: Lower metric = Lower rank)
            RANK() over (
                partition by absolute_quarter order by delta_gpoa ASC
            ) as delta_gpoa_rank,
            RANK() over (
                partition by absolute_quarter order by delta_roe ASC
            ) as delta_roe_rank,
            RANK() over (
                partition by absolute_quarter order by delta_roa ASC
            ) as delta_roa_rank,
            RANK() over (
                partition by absolute_quarter order by delta_cfoa ASC
            ) as delta_cfoa_rank,
            RANK() over (
                partition by absolute_quarter order by delta_gmar ASC
            ) as delta_gmar_rank

        from base_metrics
    ),

    -- STEP 3: Standardize ranks into Z-Scores (Mean = 0, StdDev = 1)
    z_growth_components as (
        select
            *,
            (delta_gpoa_rank - AVG(delta_gpoa_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(delta_gpoa_rank) over w_qtr, 0) as z_delta_gpoa,
            (delta_roe_rank - AVG(delta_roe_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(delta_roe_rank) over w_qtr, 0) as z_delta_roe,
            (delta_roa_rank - AVG(delta_roa_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(delta_roa_rank) over w_qtr, 0) as z_delta_roa,
            (delta_cfoa_rank - AVG(delta_cfoa_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(delta_cfoa_rank) over w_qtr, 0) as z_delta_cfoa,
            (delta_gmar_rank - AVG(delta_gmar_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(delta_gmar_rank) over w_qtr, 0) as z_delta_gmar
        from ranked_growth
        window w_qtr as (partition by absolute_quarter)
    ),

    -- STEP 4: Apply Data Quality Rules to ensure all Z-Scores are calculated
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case
                        when z_delta_gpoa is NULL then 'z_delta_gpoa is null' else NULL
                    end,
                    case
                        when z_delta_roe is NULL then 'z_delta_roe is null' else NULL
                    end,
                    case
                        when z_delta_roa is NULL then 'z_delta_roa is null' else NULL
                    end,
                    case
                        when z_delta_cfoa is NULL then 'z_delta_cfoa is null' else NULL
                    end,
                    case
                        when z_delta_gmar is NULL then 'z_delta_gmar is null' else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from z_growth_components
    )

-- STEP 5: Final Selection, Status Resolution, and Audit Injection
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    -- Output the 5 standardized Z-Scores
    z_delta_gpoa,
    z_delta_roe,
    z_delta_roa,
    z_delta_cfoa,
    z_delta_gmar,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:48:04.916793+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , 'd5f144b3-ec78-4c38-93a0-f54d53bb219b' as int_invocation_id 

from applied_dq_rules