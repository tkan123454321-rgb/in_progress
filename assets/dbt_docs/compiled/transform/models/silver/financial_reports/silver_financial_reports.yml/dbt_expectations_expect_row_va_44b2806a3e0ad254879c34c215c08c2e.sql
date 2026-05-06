with
    max_recency as (

        select max(cast(silver_updated_at as timestamp(3))) as max_timestamp
        from "lakehouse_main"."silver"."silver_cf_quarter"
        where
            -- to exclude erroneous future dates
            cast(silver_updated_at as timestamp(3)) <= cast(
                (
                    at_timezone(
                        with_timezone(cast(current_timestamp as timestamp), 'UTC'),
                        'Asia/Ho_Chi_Minh'
                    )
                ) as timestamp
            )

    )
select *
from max_recency
where
    -- if the row_condition excludes all rows, we need to compare against a default date
    -- to avoid false negatives
    coalesce(max_timestamp, cast('1970-01-01' as timestamp(3))) < cast(
        date_add(
            'day',
            -32,
            cast(
                (
                    at_timezone(
                        with_timezone(cast(current_timestamp as timestamp), 'UTC'),
                        'Asia/Ho_Chi_Minh'
                    )
                ) as timestamp
            )
        ) as timestamp(3)
    )
