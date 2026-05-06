






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and year >= 2000 and year <= 2050
)
 as expression


    from "lakehouse_main"."silver"."silver_historical_quotes"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







