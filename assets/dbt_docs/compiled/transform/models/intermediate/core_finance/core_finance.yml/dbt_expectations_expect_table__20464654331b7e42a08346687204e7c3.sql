with
    relation_columns as (

        select cast('TICKER' as varchar) as relation_column
        union all

        select cast('YEAR' as varchar) as relation_column
        union all

        select cast('QUARTER' as varchar) as relation_column
        union all

        select cast('ABSOLUTE_QUARTER' as varchar) as relation_column
        union all

        select cast('GROSS_REVENUE_TTM' as varchar) as relation_column
        union all

        select cast('NET_REVENUE_TTM' as varchar) as relation_column
        union all

        select cast('COGS_TTM' as varchar) as relation_column
        union all

        select cast('GROSS_PROFIT_TTM' as varchar) as relation_column
        union all

        select cast('PROFIT_BEFORE_TAX_TTM' as varchar) as relation_column
        union all

        select cast('INTEREST_EXPENSE_TTM' as varchar) as relation_column
        union all

        select cast('NET_INCOME_TTM' as varchar) as relation_column
        union all

        select cast('NET_INCOME_PARENT_TTM' as varchar) as relation_column
        union all

        select cast('CFO_TTM' as varchar) as relation_column
        union all

        select cast('DEPRECIATION_TTM' as varchar) as relation_column
        union all

        select cast('CAPEX_TTM' as varchar) as relation_column
        union all

        select cast('TOTAL_ASSETS' as varchar) as relation_column
        union all

        select cast('TOTAL_EQUITY' as varchar) as relation_column
        union all

        select cast('CURRENT_ASSETS' as varchar) as relation_column
        union all

        select cast('CURRENT_LIABILITIES' as varchar) as relation_column
        union all

        select cast('CASH_AND_EQUIVALENTS' as varchar) as relation_column
        union all

        select cast('INCOME_TAXES_PAYABLE' as varchar) as relation_column
        union all

        select cast('MINORITY_INTEREST' as varchar) as relation_column
        union all

        select cast('TOTAL_LIABILITIES' as varchar) as relation_column
        union all

        select cast('SHORT_TERM_DEBT' as varchar) as relation_column
        union all

        select cast('LONG_TERM_DEBT' as varchar) as relation_column
        union all

        select cast('MARKET_CAP' as varchar) as relation_column
        union all

        select cast('SHARES_OUTSTANDING' as varchar) as relation_column
        union all

        select cast('PREFERRED_STOCK' as varchar) as relation_column
        union all

        select cast('RETAINED_EARNINGS' as varchar) as relation_column
        union all

        select cast('RISK_FREE_RATE' as varchar) as relation_column
        union all

        select cast('CPI_INDEX' as varchar) as relation_column
        union all

        select cast('QUARTER_GAP' as varchar) as relation_column
        union all

        select cast('TTM_STATUS' as varchar) as relation_column
        union all

        select cast('INT_UPDATED_AT' as varchar) as relation_column
        union all

        select cast('INT_INVOCATION_ID' as varchar) as relation_column

    ),
    input_columns as (

        select cast('INT_INVOCATION_ID' as varchar) as input_column
        union all

        select cast('INT_UPDATED_AT' as varchar) as input_column

    )
select *
from input_columns i
left join relation_columns r on r.relation_column = i.input_column
where
    -- catch any column in input list that is not in the list of table columns
    r.relation_column is null
