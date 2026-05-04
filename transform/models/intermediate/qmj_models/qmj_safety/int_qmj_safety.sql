{{ config(materialized="table", tags=["intermediate", "qmj_safety"]) }}
{% set audit_cols = get_audit_columns("intermediate") %}
-- STEP 1: Extract qualified components from prior intermediate models
with
    ttm_metrics as (
        select * from {{ ref("int_ttm_metrics") }} where ttm_status = 'valid_ttm'
    ),
    bab_score as (
        select * from {{ ref("int_qmj_beta_final") }} where status = 'qualified'
    ),

    z_score as (select * from {{ ref("int_z_score") }} where status = 'qualified'),

    o_score as (select * from {{ ref("int_o_score") }} where status = 'qualified'),

    -- STEP 2: Combine all safety components using FULL OUTER JOIN
    joined_all as (
        select
            ticker,
            year,
            quarter,
            absolute_quarter,

            b.bab_score,
            z.altman_z_score,
            o.ohlson_o_score,

            t.total_assets,
            t.net_income_parent_ttm,
            (t.total_equity - t.minority_interest - t.preferred_stock) as book_equity,
            t.short_term_debt as short_term_debt,
            t.long_term_debt as long_term_debt,
            t.minority_interest as minority_interest,
            t.preferred_stock as preferred_stock

        from ttm_metrics t
        full outer join bab_score b using (ticker, year, quarter, absolute_quarter)
        full outer join z_score z using (ticker, year, quarter, absolute_quarter)
        full outer join o_score o using (ticker, year, quarter, absolute_quarter)
    ),

    -- STEP 3: Calculate Leverage (LEV) and Return on Equity (ROE)
    calc_lev_and_roe as (
        select
            *,
            -- Leverage Score = -(Total Debt) / Total Assets
            - (short_term_debt + long_term_debt + minority_interest + preferred_stock)
            / NULLIF(total_assets, 0) as lev_score,

            -- ROE is calculated here as an input for EVOL
            net_income_parent_ttm / NULLIF(book_equity, 0) as roe
        from joined_all
    ),

    -- STEP 4: Calculate Earnings Volatility (EVOL) over the past 16 quarters
    calc_evol as (
        select
            *,
            -- Raw EVOL: Standard deviation of ROE
            STDDEV_SAMP(roe) over w_16q as evol_raw,

            -- Count available quarters to enforce the minimum 12-quarter rule
            COUNT(roe) over w_16q as count_roe_quarters
        from calc_lev_and_roe
        window
            w_16q as (
                partition by ticker
                order by absolute_quarter
                rows between 15 PRECEDING and CURRENT ROW
            )
    ),

    -- STEP 5: Apply Data Quality Rules for the combined Safety factor
    applied_dq_rules as (
        select
            *,
            -- EVOL Score is the negative of the raw volatility
            (-1 * evol_raw) as evol_score,

            -- Embedded DQ checks
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Check EVOL history
                    case
                        when count_roe_quarters < 12
                        then 'Err: Not enough ROE history for EVOL (<12 quarters)'
                        else NULL
                    end,

                    -- Check 5 core safety components for nulls
                    case when bab_score is NULL then 'missing_bab_score' else NULL end,
                    case
                        when altman_z_score is NULL
                        then 'missing_altman_z_score'
                        else NULL
                    end,
                    case
                        when ohlson_o_score is NULL
                        then 'missing_ohlson_o_score'
                        else NULL
                    end,
                    case when lev_score is NULL then 'missing_lev_score' else NULL end,
                    case when evol_raw is NULL then 'missing_evol' else NULL end
                ),
                ''
            ) as unqualified_reason
        from calc_evol
    )
select
    ticker,
    year,
    quarter,
    absolute_quarter,

    bab_score,
    lev_score,
    ohlson_o_score,
    altman_z_score,
    evol_score,

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    unqualified_reason

    -- Audit Columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules
