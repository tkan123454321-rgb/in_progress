with
    grouped_expression as (
        select count(distinct int_invocation_id) = 1 as expression

        from "lakehouse_main"."intermediate"."int_qmj_scoring_growth"

    ),
    validation_errors as (

        select * from grouped_expression where not (expression = true)

    )

select *
from validation_errors
