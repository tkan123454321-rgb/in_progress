



-- STEP 1: Extract the 3 core pillars of Quality from the Gold layer
with
    gold_profitability as (select * from "lakehouse_main"."gold"."gold_qmj_z_profitability"),

    gold_growth as (select * from "lakehouse_main"."gold"."gold_qmj_z_growth"),

    gold_safety as (select * from "lakehouse_main"."gold"."gold_qmj_z_safety"),

    -- STEP 2: Combine the 3 pillars (Clean join using USING)
    joined_scores as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,

            p.qmj_profitability_score,
            g.qmj_growth_score,
            s.qmj_safety_score,

            -- The sum will automatically be NULL if any of the 3 components is NULL
            (
                p.qmj_profitability_score + g.qmj_growth_score + s.qmj_safety_score
            ) as raw_qmj_sum

        from gold_profitability p
        full outer join gold_growth g using (ticker, year, quarter, absolute_quarter)
        full outer join gold_safety s using (ticker, year, quarter, absolute_quarter)
    ),

    -- STEP 3: Rank the aggregated QMJ sums cross-sectionally per quarter
    final_ranking as (
        select
            *,
            -- Rank only valid records; missing components result in a NULL rank
            case
                when raw_qmj_sum is not NULL then RANK() over w_qtr_asc else NULL
            end as final_rank
        from joined_scores
        window w_qtr_asc as (partition by absolute_quarter order by raw_qmj_sum ASC)
    ),

    -- STEP 4: Calculate the ultimate AQR QMJ Score (Z-Score of the ranks)
    final_z_score as (
        select
            *,
            (final_rank - AVG(final_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(final_rank) over w_qtr, 0) as qmj_score
        from final_ranking
        window w_qtr as (partition by absolute_quarter)
    ),

    -- STEP 5: Apply inline Data Quality Rules for QMJ Final
    applied_dq_rules as (
        select
            *,
            -- Inline DQ Check: Ensure all 3 quality pillars are present
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case
                        when qmj_profitability_score is NULL
                        then 'Missing Profitability Score'
                        else NULL
                    end,
                    case
                        when qmj_growth_score is NULL
                        then 'Missing Growth Score'
                        else NULL
                    end,
                    case
                        when qmj_safety_score is NULL
                        then 'Missing Safety Score'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from final_z_score
    )

-- STEP 6: Final Selection, Status Resolution, and Audit Injection
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    qmj_profitability_score,
    qmj_growth_score,
    qmj_safety_score,
    qmj_score,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as gold_updated_at , '4ff423e7-7675-4eec-a090-58bdf9560b12' as gold_invocation_id 

from applied_dq_rules
order by absolute_quarter DESC, qmj_score DESC