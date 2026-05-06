select absolute_quarter as unique_field, count(*) as n_records

from "lakehouse_main"."seeds"."macro_risk_free_rate"
where absolute_quarter is not null
group by absolute_quarter
having count(*) > 1
