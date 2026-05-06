with
    validation_errors as (

        select ticker, date, count(*) as "n_records"
        from "lakehouse_main"."silver"."silver_historical_quotes"
        where 1 = 1 and not (ticker is null and date is null)

        group by ticker, date
        having count(*) > 1

    )
select *
from validation_errors
