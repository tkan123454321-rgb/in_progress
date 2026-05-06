with
    validation_errors as (

        select ticker, year, quarter, count(*) as "n_records"
        from "lakehouse_main"."silver"."silver_fundamental_quarter"
        where 1 = 1 and not (ticker is null and year is null and quarter is null)

        group by ticker, year, quarter
        having count(*) > 1

    )
select *
from validation_errors
