{{ config(materialized="table", tags=["intermediate", "qmj_profitability"]) }}

{% set audit_cols = get_audit_columns("intermediate") %}

-- STEP 1: Extract valid TTM metrics and calculate Book Equity & Working Capital
with
    base_metrics as (
        select
            *,
            (total_equity - minority_interest - preferred_stock) as book_equity,
            (
                current_assets
                - current_liabilities
                - cash_and_equivalents
                + short_term_debt
                + income_taxes_payable
            ) as working_capital
        from {{ ref("int_ttm_metrics") }}
        where ttm_status = 'valid_ttm'
    ),

    -- STEP 2: Calculate 1-year change in Working Capital and check continuity
    calc_delta_wc as (
        select
            *,
            (
                working_capital - LAG(working_capital, 4) over w_ticker
            ) as delta_working_capital,
            (
                absolute_quarter - LAG(absolute_quarter, 4) over w_ticker
            ) as quarter_gap_wc
        from base_metrics
        window w_ticker as (partition by ticker order by absolute_quarter)
    ),

    -- STEP 3: Calculate the 6 core Profitability metrics
    calc_profitability_scores as (
        select
            *,
            (gross_profit_ttm / NULLIF(total_assets, 0)) as gpoa,
            (net_income_parent_ttm / NULLIF(book_equity, 0)) as roe,
            (net_income_ttm / NULLIF(total_assets, 0)) as roa,
            (gross_profit_ttm / NULLIF(net_revenue_ttm, 0)) as gmar,
            (
                (net_income_ttm + depreciation_ttm - delta_working_capital - capex_ttm)
                / NULLIF(total_assets, 0)
            ) as cfoa,
            (
                (depreciation_ttm - delta_working_capital) / NULLIF(total_assets, 0)
            ) as acc
        from calc_delta_wc
    ),

    -- STEP 4: Apply Data Quality Rules specific to Profitability factors
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Continuity Check: Must have a 4-quarter gap for Delta WC
                    case
                        when quarter_gap_wc is NULL or quarter_gap_wc != 4
                        then 'Err: Missing historical quarters for Delta WC (Gap != 4)'
                        else NULL
                    end,

                    -- Null Checks for the 6 core components
                    case when gpoa is NULL then 'gpoa is null' else NULL end,
                    case when roe is NULL then 'roe is null' else NULL end,
                    case when roa is NULL then 'roa is null' else NULL end,
                    case when gmar is NULL then 'gmar is null' else NULL end,
                    case when cfoa is NULL then 'cfoa is null' else NULL end,
                    case when acc is NULL then 'acc is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from calc_profitability_scores
    )

-- STEP 5: Final Selection and Status Resolution
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    gpoa,
    roe,
    roa,
    gmar,
    cfoa,
    acc,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules
