with
    all_values as (

        select quarter as value_field from "lakehouse_main"."silver"."silver_bl_quarter"

    ),
    set_values as (

        select 1 as value_field
        union all
        select 2 as value_field
        union all
        select 3 as value_field
        union all
        select 4 as value_field

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
