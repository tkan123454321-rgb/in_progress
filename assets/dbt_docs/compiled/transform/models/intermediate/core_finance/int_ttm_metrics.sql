-- STEP 1: Extract qualified financial data and calculate absolute quarter
with
    __dbt__cte__int_financial_report_joined as (

        -- STEP 0: select valid companies from the dimension table
        with
            valid_companies as (
                select ticker
                from "lakehouse_main"."gold"."gold_dim_company"
                where status = 'qualified'
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
                    fund.has_fund,
                    -- 6. THÊM AUDIT COLUMNS TỰ ĐỘNG
                    CAST(
                        from_iso8601_timestamp(
                            '2026-05-06T08:01:34.665195+00:00'
                        ) as TIMESTAMP
                        with TIME ZONE
                    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at,
                    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as int_invocation_id

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
                                when has_is is NULL
                                then 'missing_income_statement'
                                else NULL
                            end,
                            case
                                when has_bs is NULL
                                then 'missing_balance_sheet'
                                else NULL
                            end,
                            case
                                when has_cf is NULL then 'missing_cash_flow' else NULL
                            end,
                            case
                                when has_fund is NULL
                                then 'missing_fundamental'
                                else NULL
                            end
                        ),
                        ''
                    ) as unqualified_reason
                from joined_data
            )

        -- STEP 4: Finalize the model, define status, and inject audit columns
        select
            -- Select all business columns (excluding temporary presence flags like
            -- has_is)
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
            end as status,
            CAST(
                from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
                with TIME ZONE
            ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at,
            'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as int_invocation_id

        from applied_dq_rules
    ),
    base_financials as (
        select *, (year * 4 + quarter) as absolute_quarter
        from __dbt__cte__int_financial_report_joined
        where status = 'qualified'
    ),

    -- STEP 2: Extract macro-economic data
    macro_data as (
        select absolute_quarter, risk_free_rate, cpi_index
        from "lakehouse_main"."seeds"."macro_risk_free_rate"
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
    d.ttm_status,
    -- Auto-generated audit columns
    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at,
    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as int_invocation_id

from applied_dq_rules d
left join macro_data m on d.absolute_quarter = m.absolute_quarter
