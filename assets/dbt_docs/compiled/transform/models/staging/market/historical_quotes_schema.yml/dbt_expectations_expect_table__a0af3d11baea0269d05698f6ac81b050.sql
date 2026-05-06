with
    relation_columns as (

        select cast('TICKER' as varchar) as relation_column
        union all

        select cast('DATE' as varchar) as relation_column
        union all

        select cast('PRICE_BASIC' as varchar) as relation_column
        union all

        select cast('PRICE_OPEN' as varchar) as relation_column
        union all

        select cast('PRICE_HIGH' as varchar) as relation_column
        union all

        select cast('PRICE_LOW' as varchar) as relation_column
        union all

        select cast('PRICE_CLOSE' as varchar) as relation_column
        union all

        select cast('TOTAL_VOLUME' as varchar) as relation_column
        union all

        select cast('TOTAL_VALUE' as varchar) as relation_column
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
