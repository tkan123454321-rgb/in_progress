




    with grouped_expression as (
    select
        
        
    
  gold_unqualified_reason is null as expression


    from "lakehouse_main"."gold"."gold_dim_company"
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



