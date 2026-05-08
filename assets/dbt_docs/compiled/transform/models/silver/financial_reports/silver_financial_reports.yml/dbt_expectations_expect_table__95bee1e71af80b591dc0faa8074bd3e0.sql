with relation_columns as (

        
        select cast('TICKER' as varchar) as relation_column
        union all
        
        select cast('YEAR' as varchar) as relation_column
        union all
        
        select cast('QUARTER' as varchar) as relation_column
        union all
        
        select cast('CFO' as varchar) as relation_column
        union all
        
        select cast('CFI' as varchar) as relation_column
        union all
        
        select cast('CFF' as varchar) as relation_column
        union all
        
        select cast('NET_CASH_FLOW' as varchar) as relation_column
        union all
        
        select cast('DEPRECIATION' as varchar) as relation_column
        union all
        
        select cast('CAPEX' as varchar) as relation_column
        union all
        
        select cast('PROFIT_BEFORE_TAX_CF' as varchar) as relation_column
        union all
        
        select cast('DELTA_RECEIVABLES' as varchar) as relation_column
        union all
        
        select cast('DELTA_INVENTORY' as varchar) as relation_column
        union all
        
        select cast('DELTA_PAYABLES' as varchar) as relation_column
        union all
        
        select cast('BORROWINGS_RECEIVED' as varchar) as relation_column
        union all
        
        select cast('DEBT_REPAID' as varchar) as relation_column
        union all
        
        select cast('DIVIDENDS_PAID' as varchar) as relation_column
        union all
        
        select cast('BEGINNING_CASH' as varchar) as relation_column
        union all
        
        select cast('EXCHANGE_RATE_EFFECT' as varchar) as relation_column
        union all
        
        select cast('ENDING_CASH' as varchar) as relation_column
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