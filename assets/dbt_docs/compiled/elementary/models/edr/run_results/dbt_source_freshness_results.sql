


    select * from (
            select
        cast(
            'dummy_string' as varchar
        ) as source_freshness_execution_id

,
        cast(
            'dummy_string' as varchar
        ) as unique_id

,
        cast(
            'dummy_string' as varchar
        ) as max_loaded_at

,
        cast(
            'dummy_string' as varchar
        ) as snapshotted_at

,
        cast(
            'dummy_string' as varchar
        ) as generated_at

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as created_at

,
        cast(
            123456789.99 as double
        ) as max_loaded_at_time_ago_in_s

,
        cast(
            'dummy_string' as varchar
        ) as status

,
        cast(
            'dummy_string' as varchar
        ) as error

,
        cast(
            'dummy_string' as varchar
        ) as compile_started_at

,
        cast(
            'dummy_string' as varchar
        ) as compile_completed_at

,
        cast(
            'dummy_string' as varchar
        ) as execute_started_at

,
        cast(
            'dummy_string' as varchar
        ) as execute_completed_at

,
        cast(
            'dummy_string' as varchar
        ) as invocation_id

,
        cast(
            'dummy_string' as varchar
        ) as warn_after

,
        cast(
            'dummy_string' as varchar
        ) as error_after

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as filter


        ) as empty_table
        where 1 = 0
