with
    child as (
        select ticker as from_field
        from
            (
                select *
                from "lakehouse_main"."silver"."silver_historical_quotes"
                where ticker != 'VNINDEX'
            ) dbt_subquery
        where ticker is not null
    ),

    parent as (
        select ticker as to_field from "lakehouse_main"."gold"."gold_dim_company"
    )

select from_field

from child
left join parent on child.from_field = parent.to_field

where parent.to_field is null
