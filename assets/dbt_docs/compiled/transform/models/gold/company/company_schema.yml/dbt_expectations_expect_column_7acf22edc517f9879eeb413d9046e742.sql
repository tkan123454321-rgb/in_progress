






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and avg_volume_3m >= 10000
)
 as expression


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







