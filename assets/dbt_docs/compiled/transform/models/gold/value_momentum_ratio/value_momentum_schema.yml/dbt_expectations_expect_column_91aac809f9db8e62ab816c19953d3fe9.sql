


    with grouped_expression as (
    select
        
        
    
  
count(distinct gold_invocation_id) = 1
 as expression


    from "lakehouse_main"."gold"."gold_value_and_momentum_z"
    

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


