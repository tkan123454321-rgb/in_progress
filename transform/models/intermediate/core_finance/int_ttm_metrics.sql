{{ config(materialized='table',
    tags=['intermediate', 'ttm_metrics']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}
-- STEP 1: Extract qualified financial data and calculate absolute quarter
WITH base_financials AS (
    SELECT 
        *,
        (year * 4 + quarter) AS absolute_quarter
    FROM {{ ref('int_financial_report_joined') }}
    WHERE status = 'qualified'
),

-- STEP 2: Extract macro-economic data
macro_data AS (
    SELECT 
        absolute_quarter,
        risk_free_rate,
        cpi_index
    FROM {{ ref('macro_risk_free_rate') }}
),

-- STEP 3: Apply window functions to calculate Trailing Twelve Months (TTM) sums
ttm_calculations AS (
    SELECT 
        -- Basic Identifiers
        ticker,
        year,
        quarter,
        absolute_quarter,

        -- Income Statement TTM (Sum of the last 4 consecutive quarters)
        SUM(gross_revenue) OVER w_ttm AS gross_revenue_ttm,
        SUM(net_revenue) OVER w_ttm AS net_revenue_ttm,
        SUM(cogs) OVER w_ttm AS cogs_ttm,
        SUM(gross_profit) OVER w_ttm AS gross_profit_ttm,
        SUM(profit_before_tax) OVER w_ttm AS profit_before_tax_ttm,
        SUM(interest_expense) OVER w_ttm AS interest_expense_ttm,
        SUM(net_income) OVER w_ttm AS net_income_ttm,
        SUM(net_income_parent) OVER w_ttm AS net_income_parent_ttm,

        -- Cash Flow TTM (Sum of the last 4 consecutive quarters)
        SUM(cfo) OVER w_ttm AS cfo_ttm,
        SUM(depreciation) OVER w_ttm AS depreciation_ttm,
        SUM(capex) OVER w_ttm AS capex_ttm,

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

    FROM base_financials
    WINDOW 
        w_ttm AS (PARTITION BY ticker ORDER BY absolute_quarter ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),

-- STEP 4: Apply Data Quality Rules for TTM continuity
applied_dq_rules AS (
    SELECT 
        *,
        -- Continuity Check: Gap between current quarter and the quarter 3 rows ago
        (absolute_quarter - LAG(absolute_quarter, 3) OVER w_window) AS quarter_gap,
        
        CASE 
            WHEN (absolute_quarter - LAG(absolute_quarter, 3) OVER w_window) = 3 THEN 'valid_ttm'
            ELSE 'broken_ttm' 
        END AS ttm_status

    FROM ttm_calculations
    WINDOW 
        w_window AS (PARTITION BY ticker ORDER BY absolute_quarter)
)

-- STEP 5: Finalize the model, join macro data, and inject metadata
SELECT 
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
        {% for col in audit_cols %}
        , {{ col.expr }} AS {{ col.alias }}
        {% endfor %}

FROM applied_dq_rules d
LEFT JOIN macro_data m 
    ON d.absolute_quarter = m.absolute_quarter

