



-- STEP 1: Extract qualified raw safety metrics from the ephemeral layer
with
    base_metrics as (
        select * from "lakehouse_main"."intermediate"."int_qmj_safety" where status = 'qualified'
    ),

    -- STEP 2: Rank each component cross-sectionally per quarter
    ranked_safety as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,

            -- 1. BAB (Betting Against Beta)
            RANK() over (
                partition by absolute_quarter order by bab_score ASC
            ) as bab_rank,

            -- 2. LEV (Leverage)
            RANK() over (
                partition by absolute_quarter order by lev_score ASC
            ) as lev_rank,

            -- 3. Ohlson O-Score
            RANK() over (
                partition by absolute_quarter order by ohlson_o_score ASC
            ) as o_rank,

            -- 4. Altman Z-Score
            RANK() over (
                partition by absolute_quarter order by altman_z_score ASC
            ) as z_rank,

            -- 5. EVOL (Earnings Volatility)
            RANK() over (
                partition by absolute_quarter order by evol_score ASC
            ) as evol_rank

        from base_metrics
    ),
    -- STEP 3: Standardize ranks into Z-Scores
    z_safety_components as (
        select
            *,
            (bab_rank - AVG(bab_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(bab_rank) over w_qtr, 0) as z_bab,
            (lev_rank - AVG(lev_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(lev_rank) over w_qtr, 0) as z_lev,
            (o_rank - AVG(o_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(o_rank) over w_qtr, 0) as z_o,
            (z_rank - AVG(z_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(z_rank) over w_qtr, 0) as z_z,
            (evol_rank - AVG(evol_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(evol_rank) over w_qtr, 0) as z_evol
        from ranked_safety
        window w_qtr as (partition by absolute_quarter)
    ),

    -- STEP 4: Apply Data Quality Rules to ensure all Z-Scores are calculated
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case when z_bab is NULL then 'z_bab is null' else NULL end,
                    case when z_lev is NULL then 'z_lev is null' else NULL end,
                    case when z_o is NULL then 'z_o is null' else NULL end,
                    case when z_z is NULL then 'z_z is null' else NULL end,
                    case when z_evol is NULL then 'z_evol is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from z_safety_components
    )

-- STEP 5: Final Selection, Status Resolution, and Audit Injection
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    -- Output the 5 standardized Z-Scores
    z_bab,
    z_lev,
    z_o,
    z_z,
    z_evol,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , '4c6d9271-375a-4d96-926e-49714c96b216' as int_invocation_id 

from applied_dq_rules