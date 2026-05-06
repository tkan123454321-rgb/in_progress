with
    relation_columns as (

        select cast('TICKER' as varchar) as relation_column
        union all

        select cast('YEAR' as varchar) as relation_column
        union all

        select cast('QUARTER' as varchar) as relation_column
        union all

        select cast('GROSS_REVENUE' as varchar) as relation_column
        union all

        select cast('REVENUE_DEDUCTION' as varchar) as relation_column
        union all

        select cast('NET_REVENUE' as varchar) as relation_column
        union all

        select cast('COGS' as varchar) as relation_column
        union all

        select cast('GROSS_PROFIT' as varchar) as relation_column
        union all

        select cast('FINANCIAL_INCOME' as varchar) as relation_column
        union all

        select cast('FINANCIAL_EXPENSE' as varchar) as relation_column
        union all

        select cast('INTEREST_EXPENSE' as varchar) as relation_column
        union all

        select cast('AFFILIATE_PROFIT' as varchar) as relation_column
        union all

        select cast('SELLING_EXPENSE' as varchar) as relation_column
        union all

        select cast('ADMIN_EXPENSE' as varchar) as relation_column
        union all

        select cast('OPERATING_PROFIT' as varchar) as relation_column
        union all

        select cast('OTHER_INCOME' as varchar) as relation_column
        union all

        select cast('OTHER_EXPENSE' as varchar) as relation_column
        union all

        select cast('OTHER_PROFIT' as varchar) as relation_column
        union all

        select cast('PROFIT_BEFORE_TAX' as varchar) as relation_column
        union all

        select cast('CURRENT_TAX' as varchar) as relation_column
        union all

        select cast('DEFERRED_TAX' as varchar) as relation_column
        union all

        select cast('INCOME_TAX_EXPENSE' as varchar) as relation_column
        union all

        select cast('NET_INCOME' as varchar) as relation_column
        union all

        select cast('MINORITY_INTEREST' as varchar) as relation_column
        union all

        select cast('NET_INCOME_PARENT' as varchar) as relation_column
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
