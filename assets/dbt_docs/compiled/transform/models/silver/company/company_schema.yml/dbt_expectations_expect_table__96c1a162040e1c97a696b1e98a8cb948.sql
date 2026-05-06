with
    relation_columns as (

        select cast('TICKER' as varchar) as relation_column
        union all

        select cast('COMPANY_NAME' as varchar) as relation_column
        union all

        select cast('INDUSTRY_GROUP' as varchar) as relation_column
        union all

        select cast('SECTOR_DETAIL' as varchar) as relation_column
        union all

        select cast('COMPANY_TYPE' as varchar) as relation_column
        union all

        select cast('SILVER_UPDATED_AT' as varchar) as relation_column
        union all

        select cast('SILVER_INVOCATION_ID' as varchar) as relation_column

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
