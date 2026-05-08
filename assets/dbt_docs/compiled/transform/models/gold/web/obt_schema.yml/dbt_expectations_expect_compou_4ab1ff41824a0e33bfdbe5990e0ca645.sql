



with validation_errors as (

    select
        ticker,absolute_quarter,
        count(*) as "n_records"
    from "lakehouse_main"."gold"."obt_web"
    where
        1=1
        and 
    not (
        ticker is null and 
        absolute_quarter is null
        
    )


    
    group by
        ticker,absolute_quarter
    having count(*) > 1

)
select * from validation_errors
