




    with grouped_expression as (
    select
        
        
    
  


    
    
    
    if(regexp_position(ticker, '^[A-Z0-9]{3}$', 1, 1) = -1, 0, regexp_position(ticker, '^[A-Z0-9]{3}$', 1, 1))


 > 0
 as expression


    from "lakehouse_main"."silver"."silver_fundamental_quarter"
    

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




