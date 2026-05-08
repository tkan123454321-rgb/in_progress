with relation_columns as (

        
        select cast('TICKER' as varchar) as relation_column
        union all
        
        select cast('YEAR' as varchar) as relation_column
        union all
        
        select cast('QUARTER' as varchar) as relation_column
        union all
        
        select cast('PREFERRED_STOCK' as varchar) as relation_column
        union all
        
        select cast('MARKET_CAP' as varchar) as relation_column
        union all
        
        select cast('SHARES_OUTSTANDING' as varchar) as relation_column
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
    from
        input_columns i
        left join
        relation_columns r on r.relation_column = i.input_column
    where
        -- catch any column in input list that is not in the list of table columns
        r.relation_column is null