with
    all_values as (

        select status as value_field, count(*) as n_records

        from "lakehouse_main"."gold"."gold_qmj_z_final"
        group by status

    )

select *
from all_values
where value_field not in ('qualified', 'unqualified')
