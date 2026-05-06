with
    child as (
        select ticker as from_field
        from "lakehouse_main"."silver"."silver_dim_company"
        where ticker is not null
    ),

    parent as (
        select ticker as to_field from "lakehouse_main"."seeds"."bronze_dim_company"
    )

select from_field

from child
left join parent on child.from_field = parent.to_field

where parent.to_field is null
