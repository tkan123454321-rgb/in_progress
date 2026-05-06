with
    all_values as (

        select exchange as value_field

        from "lakehouse_main"."gold"."gold_dim_company"

        where status = 'qualified'

    ),
    set_values as (

        select cast('HNX' as varchar) as value_field
        union all
        select cast('UPCOM' as varchar) as value_field
        union all
        select cast('HSX' as varchar) as value_field

    ),
    validation_errors as (
        -- values from the model that are not in the set
        select v.value_field
        from all_values v
        left join set_values s on v.value_field = s.value_field
        where s.value_field is null

    )

select *
from validation_errors
