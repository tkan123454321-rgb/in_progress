


    with grouped_expression as (
    select
        
        
    
  
count(distinct silver_invocation_id) = 1
 as expression


    from "lakehouse_main"."silver"."silver_dividend"
    

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


