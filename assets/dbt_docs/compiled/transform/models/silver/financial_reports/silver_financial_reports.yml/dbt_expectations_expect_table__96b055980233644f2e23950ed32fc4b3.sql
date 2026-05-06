with
    relation_columns as (

        select cast('TICKER' as varchar) as relation_column
        union all

        select cast('YEAR' as varchar) as relation_column
        union all

        select cast('QUARTER' as varchar) as relation_column
        union all

        select cast('TOTAL_ASSETS' as varchar) as relation_column
        union all

        select cast('TOTAL_CAPITAL' as varchar) as relation_column
        union all

        select cast('CURRENT_ASSETS' as varchar) as relation_column
        union all

        select cast('LONG_TERM_ASSETS' as varchar) as relation_column
        union all

        select cast('TOTAL_LIABILITIES' as varchar) as relation_column
        union all

        select cast('TOTAL_EQUITY' as varchar) as relation_column
        union all

        select cast('CURRENT_LIABILITIES' as varchar) as relation_column
        union all

        select cast('LONG_TERM_LIABILITIES' as varchar) as relation_column
        union all

        select cast('CASH_AND_EQUIVALENTS' as varchar) as relation_column
        union all

        select cast('INCOME_TAXES_PAYABLE' as varchar) as relation_column
        union all

        select cast('MINORITY_INTEREST' as varchar) as relation_column
        union all

        select cast('RETAINED_EARNINGS' as varchar) as relation_column
        union all

        select cast('FIXED_ASSETS' as varchar) as relation_column
        union all

        select cast('SHORT_TERM_RECEIVABLES' as varchar) as relation_column
        union all

        select cast('SHORT_TERM_DEBT' as varchar) as relation_column
        union all

        select cast('CURRENT_PORTION_LT_DEBT' as varchar) as relation_column
        union all

        select cast('REPO_TRANSACTIONS' as varchar) as relation_column
        union all

        select cast('LONG_TERM_DEBT' as varchar) as relation_column
        union all

        select cast('CONVERTIBLE_BONDS' as varchar) as relation_column
        union all

        select cast('SILVER_UPDATED_AT' as varchar) as relation_column
        union all

        select cast('SILVER_INVOCATION_ID' as varchar) as relation_column
        union all

        select cast('STATUS' as varchar) as relation_column
        union all

        select cast('UNQUALIFIED_REASON' as varchar) as relation_column

    ),
    input_columns as (

        select cast('SILVER_INVOCATION_ID' as varchar) as input_column
        union all

        select cast('SILVER_UPDATED_AT' as varchar) as input_column

    )
select *
from input_columns i
left join relation_columns r on r.relation_column = i.input_column
where
    -- catch any column in input list that is not in the list of table columns
    r.relation_column is null
