
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "lakehouse_main"."silver"."silver_fundamental_1"
where ticker is not null
group by ticker
having count(*) > 1


