-- STEP 1: Extract qualified raw profitability metrics
with
    base_metrics as (
        select *
        from "lakehouse_main"."intermediate"."int_qmj_profitability"
        where status = 'qualified'
    ),

    -- STEP 2: Rank each profitability component cross-sectionally per quarter
    ranked_profitability as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,

            -- Ranks (Ascending: Lower metric = Lower rank)
            RANK() over (partition by absolute_quarter order by gpoa ASC) as gpoa_rank,
            RANK() over (partition by absolute_quarter order by roe ASC) as roe_rank,
            RANK() over (partition by absolute_quarter order by roa ASC) as roa_rank,
            RANK() over (partition by absolute_quarter order by cfoa ASC) as cfoa_rank,
            RANK() over (partition by absolute_quarter order by gmar ASC) as gmar_rank,
            RANK() over (partition by absolute_quarter order by acc ASC) as acc_rank

        from base_metrics
    ),

    -- STEP 3: Standardize ranks into Z-Scores (Mean = 0, StdDev = 1)
    z_profitability_components as (
        select
            *,
            (gpoa_rank - AVG(gpoa_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(gpoa_rank) over w_qtr, 0) as z_gpoa,
            (roe_rank - AVG(roe_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(roe_rank) over w_qtr, 0) as z_roe,
            (roa_rank - AVG(roa_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(roa_rank) over w_qtr, 0) as z_roa,
            (cfoa_rank - AVG(cfoa_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(cfoa_rank) over w_qtr, 0) as z_cfoa,
            (gmar_rank - AVG(gmar_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(gmar_rank) over w_qtr, 0) as z_gmar,
            (acc_rank - AVG(acc_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(acc_rank) over w_qtr, 0) as z_acc
        from ranked_profitability
        window w_qtr as (partition by absolute_quarter)
    ),

    -- STEP 4: Apply Data Quality Rules to ensure all Z-Scores are calculated
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case when z_gpoa is NULL then 'z_gpoa is null' else NULL end,
                    case when z_roe is NULL then 'z_roe is null' else NULL end,
                    case when z_roa is NULL then 'z_roa is null' else NULL end,
                    case when z_cfoa is NULL then 'z_cfoa is null' else NULL end,
                    case when z_gmar is NULL then 'z_gmar is null' else NULL end,
                    case when z_acc is NULL then 'z_acc is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from z_profitability_components
    )

-- STEP 5: Final Selection, Status Resolution, and Audit Injection
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    -- Output the 6 standardized Z-Scores
    z_gpoa,
    z_roe,
    z_roa,
    z_cfoa,
    z_gmar,
    z_acc,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason,
    -- Auto-generated audit columns
    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at,
    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as int_invocation_id

from applied_dq_rules
