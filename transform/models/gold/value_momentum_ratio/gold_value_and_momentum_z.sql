{{ config(materialized="table", tags=["gold", "historical_value_and_momentum_z"]) }}

{% set audit_cols = get_audit_columns("gold") %}

-- STEP 1: Extract qualified historical scores for Value and Momentum
with
    base_value as (
        select * from {{ ref("int_value_ratio") }} where status = 'qualified'
    ),

    base_momentum as (
        select * from {{ ref("int_momentum_ratio") }} where status = 'qualified'
    ),

    -- STEP 2: Combine metrics cleanly using USING clause
    joined_metrics as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,
            v.value_raw_score,
            m.momentum_raw_score
        from base_value v
        full outer join base_momentum m using (ticker, year, quarter, absolute_quarter)
    ),

    -- STEP 3: Rank metrics cross-sectionally per quarter
    ranked_metrics as (
        select
            *,
            -- Rank Value: Lower score = Lower Rank
            case
                when value_raw_score is not NULL then RANK() over w_qtr_val else NULL
            end as value_rank,

            -- Rank Momentum: Lower score = Lower Rank
            case
                when momentum_raw_score is not NULL then RANK() over w_qtr_mom else NULL
            end as momentum_rank
        from joined_metrics
        window
            w_qtr_val as (partition by absolute_quarter order by value_raw_score ASC),
            w_qtr_mom as (partition by absolute_quarter order by momentum_raw_score ASC)
    ),

    -- STEP 4: Standardize ranks into Z-Scores per quarter
    z_score_components as (
        select
            *,
            -- Z-Score for Historical Value
            (value_rank - AVG(value_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(value_rank) over w_qtr, 0) as z_value,

            -- Z-Score for Historical Momentum
            (momentum_rank - AVG(momentum_rank) over w_qtr)
            / NULLIF(STDDEV_SAMP(momentum_rank) over w_qtr, 0) as z_momentum

        from ranked_metrics
        window w_qtr as (partition by absolute_quarter)
    ),

    -- STEP 5: Apply inline Data Quality Rules
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case when z_value is NULL then 'z_value is null' else NULL end,
                    case when z_momentum is NULL then 'z_momentum is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from z_score_components
    )

-- STEP 6: Final Selection, Status Resolution, and Audit Injection
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    value_raw_score,
    momentum_raw_score,

    z_value,
    z_momentum,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules
