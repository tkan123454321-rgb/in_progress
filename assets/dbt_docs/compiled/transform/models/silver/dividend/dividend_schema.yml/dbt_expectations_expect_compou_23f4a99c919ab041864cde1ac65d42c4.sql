with
    validation_errors as (

        select ticker, year, count(*) as "n_records"
        from "lakehouse_main"."silver"."silver_dividend"
        where 1 = 1 and not (ticker is null and year is null)

        group by ticker, year
        having count(*) > 1

    )
select *
from validation_errors
