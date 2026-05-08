




    with grouped_expression as (
    select
        
        
    
  qmj_growth_score is not null as expression


    from "lakehouse_main"."gold"."gold_qmj_z_final"
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



