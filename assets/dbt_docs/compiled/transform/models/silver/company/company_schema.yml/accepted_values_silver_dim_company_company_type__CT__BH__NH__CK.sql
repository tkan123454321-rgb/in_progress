
    
    

with all_values as (

    select
        company_type as value_field,
        count(*) as n_records

    from "lakehouse_main"."silver"."silver_dim_company"
    group by company_type

)

select *
from all_values
where value_field not in (
    'CT','BH','NH','CK'
)


