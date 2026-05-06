select ticker as unique_field, count(*) as n_records

from "lakehouse_main"."silver"."silver_dim_company"
where ticker is not null
group by ticker
having count(*) > 1
