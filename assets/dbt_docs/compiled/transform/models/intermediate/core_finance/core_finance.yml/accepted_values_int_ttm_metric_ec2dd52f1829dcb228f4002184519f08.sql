with
    all_values as (

        select ttm_status as value_field, count(*) as n_records

        from "lakehouse_main"."intermediate"."int_ttm_metrics"
        group by ttm_status

    )

select *
from all_values
where value_field not in ('valid_ttm', 'broken_ttm')
