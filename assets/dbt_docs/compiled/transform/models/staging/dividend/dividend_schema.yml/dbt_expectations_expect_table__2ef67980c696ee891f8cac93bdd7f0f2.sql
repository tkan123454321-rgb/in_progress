with
    relation_columns as (

        select cast('TICKER' as varchar) as relation_column
        union all

        select cast('YEAR' as varchar) as relation_column
        union all

        select cast('CASH_DIVIDEND' as varchar) as relation_column
        union all

        select cast('STOCK_DIVIDEND' as varchar) as relation_column
        union all

        select cast('BRONZE_INGESTED_TIME' as varchar) as relation_column
        union all

        select cast('STAGED_AT' as varchar) as relation_column
        union all

        select cast('STAGING_INVOCATION_ID' as varchar) as relation_column

    ),
    input_columns as (

        select cast('STAGING_INVOCATION_ID' as varchar) as input_column
        union all

        select cast('STAGED_AT' as varchar) as input_column
        union all

        select cast('BRONZE_INGESTED_TIME' as varchar) as input_column

    )
select *
from input_columns i
left join relation_columns r on r.relation_column = i.input_column
where
    -- catch any column in input list that is not in the list of table columns
    r.relation_column is null
