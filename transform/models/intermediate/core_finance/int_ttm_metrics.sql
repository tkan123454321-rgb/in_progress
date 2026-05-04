{{ config(materialized="table", tags=["intermediate", "ttm_metrics"]) }}

{% set audit_cols = get_audit_columns("intermediate") %}
-- STEP 1: Extract qualified financial data and calculate absolute quarter
with
    base_financials as (
        select *, (year * 4 + quarter) as absolute_quarter
        from {{ ref("int_financial_report_joined") }}
        where status = 'qualified'
    ),

    -- STEP 2: Extract macro-economic data
    macro_data as (
        select absolute_quarter, risk_free_rate, cpi_index
        from {{ ref("macro_risk_free_rate") }}
    ),

    -- STEP 3: Apply window functions to calculate Trailing Twelve Months (TTM) sums
    ttm_calculations as (
        select
            -- Basic Identifiers
            ticker,
            year,
            quarter,
            absolute_quarter,

            -- Income Statement TTM (Sum of the last 4 consecutive quarters)
            SUM(gross_revenue) over w_ttm as gross_revenue_ttm,
            SUM(net_revenue) over w_ttm as net_revenue_ttm,
            SUM(cogs) over w_ttm as cogs_ttm,
            SUM(gross_profit) over w_ttm as gross_profit_ttm,
            SUM(profit_before_tax) over w_ttm as profit_before_tax_ttm,
            SUM(interest_expense) over w_ttm as interest_expense_ttm,
            SUM(net_income) over w_ttm as net_income_ttm,
            SUM(net_income_parent) over w_ttm as net_income_parent_ttm,

            -- Cash Flow TTM (Sum of the last 4 consecutive quarters)
            SUM(cfo) over w_ttm as cfo_ttm,
            SUM(depreciation) over w_ttm as depreciation_ttm,
            SUM(capex) over w_ttm as capex_ttm,

            -- Balance Sheet & Fundamentals (Point-in-time metrics, pass-through)
            total_assets,
            total_equity,
            current_assets,
            current_liabilities,
            cash_and_equivalents,
            income_taxes_payable,
            minority_interest,
            total_liabilities,
            short_term_debt,
            long_term_debt,
            market_cap,
            shares_outstanding,
            preferred_stock,
            retained_earnings

        from base_financials
        window
            w_ttm as (
                partition by ticker
                order by absolute_quarter
                rows between 3 PRECEDING and CURRENT ROW
            )
    ),

    -- STEP 4: Apply Data Quality Rules for TTM continuity
    applied_dq_rules as (
        select
            *,
            -- Continuity Check: Gap between current quarter and the quarter 3 rows ago
            (absolute_quarter - LAG(absolute_quarter, 3) over w_window) as quarter_gap,

            case
                when (absolute_quarter - LAG(absolute_quarter, 3) over w_window) = 3
                then 'valid_ttm'
                else 'broken_ttm'
            end as ttm_status

        from ttm_calculations
        window w_window as (partition by ticker order by absolute_quarter)
    )

-- STEP 5: Finalize the model, join macro data, and inject metadata
select
    d.ticker,
    d.year,
    d.quarter,
    d.absolute_quarter,

    -- TTM Metrics
    d.gross_revenue_ttm,
    d.net_revenue_ttm,
    d.cogs_ttm,
    d.gross_profit_ttm,
    d.profit_before_tax_ttm,
    d.interest_expense_ttm,
    d.net_income_ttm,
    d.net_income_parent_ttm,
    d.cfo_ttm,
    d.depreciation_ttm,
    d.capex_ttm,

    -- Point-in-time Metrics
    d.total_assets,
    d.total_equity,
    d.current_assets,
    d.current_liabilities,
    d.cash_and_equivalents,
    d.income_taxes_payable,
    d.minority_interest,
    d.total_liabilities,
    d.short_term_debt,
    d.long_term_debt,
    d.market_cap,
    d.shares_outstanding,
    d.preferred_stock,
    d.retained_earnings,

    -- Macro Metrics
    m.risk_free_rate,
    m.cpi_index,

    -- Validation Status
    d.quarter_gap,
    d.ttm_status

    -- Auto-generated audit columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules d
left join macro_data m on d.absolute_quarter = m.absolute_quarter
