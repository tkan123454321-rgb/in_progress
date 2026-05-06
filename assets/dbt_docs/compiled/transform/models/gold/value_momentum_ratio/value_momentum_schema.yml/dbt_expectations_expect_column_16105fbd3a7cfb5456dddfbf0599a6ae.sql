




    with grouped_expression as (
    select
        
        
    
  z_momentum_recent is not null as expression


    from "lakehouse_main"."gold"."gold_value_and_momentum_z_recent"
    where
        status = 'qualified'
    
    

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



