{{ config(materialized='ephemeral',
    tags=['intermediate', 'financial_report_joined']
) }}

{% set audit_cols = get_audit_columns('intermediate') %}


-- STEP 1: Extract qualified data from silver layers 
WITH income_statement AS (
    SELECT *, 1 AS has_is 
    FROM {{ ref('silver_ic_quarter') }}
    WHERE status = 'qualified'
),

balance_sheet AS (
    SELECT *, 1 AS has_bs 
    FROM {{ ref('silver_bl_quarter') }}
    WHERE status = 'qualified'
),

cash_flow AS (
    SELECT *, 1 AS has_cf 
    FROM {{ ref('silver_cf_quarter') }}
    WHERE status = 'qualified'
),

fundamental AS (
    SELECT *, 1 AS has_fund 
    FROM {{ ref('silver_fundamental_quarter') }}
    WHERE status = 'qualified'
),

-- STEP 2: Combine all financial statements using FULL OUTER JOIN
joined_data AS (
    SELECT 
        ticker,
        year,
        quarter,
        
        -- Income Statement metrics
        is_stmt.gross_revenue,
        is_stmt.net_revenue,
        is_stmt.cogs,
        is_stmt.gross_profit,
        is_stmt.interest_expense,
        is_stmt.profit_before_tax,
        is_stmt.net_income,
        is_stmt.net_income_parent,

        -- Balance Sheet metrics
        bs.total_assets,
        bs.total_equity,
        bs.current_assets,
        bs.current_liabilities,
        bs.cash_and_equivalents,
        bs.income_taxes_payable,
        bs.minority_interest,
        bs.total_liabilities,
        bs.short_term_debt,
        bs.long_term_debt,
        bs.retained_earnings,

        -- Cash Flow metrics
        cf.cfo,
        cf.depreciation,
        cf.capex,

        -- Fundamental metrics
        fund.market_cap,
        fund.shares_outstanding,
        fund.preferred_stock,

        -- Presence flags for Data Quality checks
        is_stmt.has_is,
        bs.has_bs,
        cf.has_cf,
        fund.has_fund

        -- 6. THÊM AUDIT COLUMNS TỰ ĐỘNG
        {% set audit_cols = get_audit_columns('intermediate') %}
        {% for col in audit_cols %}
        , {{ col.expr }} AS {{ col.alias }}
        {% endfor %}

    FROM income_statement is_stmt
    FULL OUTER JOIN balance_sheet bs 
        USING (ticker, year, quarter)
    FULL OUTER JOIN cash_flow cf 
        USING (ticker, year, quarter)
    FULL OUTER JOIN fundamental fund 
        USING (ticker, year, quarter)
),

-- STEP 3: Evaluate completeness and generate Data Quality reasons
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN has_is IS NULL THEN 'missing_income_statement' ELSE NULL END,
                CASE WHEN has_bs IS NULL THEN 'missing_balance_sheet' ELSE NULL END,
                CASE WHEN has_cf IS NULL THEN 'missing_cash_flow' ELSE NULL END,
                CASE WHEN has_fund IS NULL THEN 'missing_fundamental' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM joined_data
)



-- STEP 4: Finalize the model, define status, and inject audit columns
SELECT 
    -- Select all business columns (excluding temporary presence flags like has_is)
    ticker,
    year,
    quarter,
    gross_revenue,
    net_revenue,
    cogs,
    gross_profit,
    interest_expense,
    profit_before_tax,
    net_income,
    net_income_parent,
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
    retained_earnings,
    cfo,
    depreciation,
    capex,
    market_cap,
    shares_outstanding,
    preferred_stock,

    unqualified_reason,

    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_dq_rules