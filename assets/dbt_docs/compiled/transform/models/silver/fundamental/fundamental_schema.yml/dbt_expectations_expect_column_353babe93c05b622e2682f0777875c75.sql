with
    grouped_expression as (
        select shares_outstanding >= floating_shares as expression

        from "lakehouse_main"."silver"."silver_fundamental_1"

    ),
    validation_errors as (

        select * from grouped_expression where not (expression = true)

    )

select *
from validation_errors
