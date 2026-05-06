



-- STEP 0: select valid companies from the dimension table
with
    valid_companies as (
        select ticker from "lakehouse_main"."gold"."gold_dim_company" where status = 'qualified'
    ),
    -- STEP 1: Extract qualified data from silver layers
    income_statement as (
        select *, 1 as has_is
        from "lakehouse_main"."silver"."silver_ic_quarter"
        where status = 'qualified'
    ),

    balance_sheet as (
        select *, 1 as has_bs
        from "lakehouse_main"."silver"."silver_bl_quarter"
        where status = 'qualified'
    ),

    cash_flow as (
        select *, 1 as has_cf
        from "lakehouse_main"."silver"."silver_cf_quarter"
        where status = 'qualified'
    ),

    fundamental as (
        select *, 1 as has_fund
        from "lakehouse_main"."silver"."silver_fundamental_quarter"
        where status = 'qualified'
    ),

    -- STEP 2: Combine all financial statements using FULL OUTER JOIN
    joined_data as (
        select
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
            
            , CAST(from_iso8601_timestamp('2026-05-06T08:48:04.916793+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , 'd5f144b3-ec78-4c38-93a0-f54d53bb219b' as int_invocation_id 

        from income_statement is_stmt
        full outer join balance_sheet bs using (ticker, year, quarter)
        full outer join cash_flow cf using (ticker, year, quarter)
        full outer join fundamental fund using (ticker, year, quarter)
        inner join valid_companies using (ticker)
    ),

    -- STEP 3: Evaluate completeness and generate Data Quality reasons
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    case
                        when has_is is NULL then 'missing_income_statement' else NULL
                    end,
                    case when has_bs is NULL then 'missing_balance_sheet' else NULL end,
                    case when has_cf is NULL then 'missing_cash_flow' else NULL end,
                    case when has_fund is NULL then 'missing_fundamental' else NULL end
                ),
                ''
            ) as unqualified_reason
        from joined_data
    )

-- STEP 4: Finalize the model, define status, and inject audit columns
select
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

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status

    , CAST(from_iso8601_timestamp('2026-05-06T08:48:04.916793+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , 'd5f144b3-ec78-4c38-93a0-f54d53bb219b' as int_invocation_id 

from applied_dq_rules