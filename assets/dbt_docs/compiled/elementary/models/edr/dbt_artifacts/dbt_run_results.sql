

select * from (
            select
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as model_execution_id

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as unique_id

,
        cast(
            'dummy_string' as varchar
        ) as invocation_id

,
        cast(
            'dummy_string' as varchar
        ) as generated_at

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as created_at

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as name

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as message

,
        cast(
            'dummy_string' as varchar
        ) as status

,
        cast(
            'dummy_string' as varchar
        ) as resource_type

,
        cast(
            123456789.99 as double
        ) as execution_time

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
        ) as compile_started_at

,
        cast(
            'dummy_string' as varchar
        ) as compile_completed_at

,
        cast(
            31474836478 as bigint
        ) as rows_affected

,
        cast(
            True as boolean
        ) as full_refresh

,
        cast(
            'this_is_just_a_long_dummy_string'
            as varchar
        ) as compiled_code

,
        cast(
            31474836478 as bigint
        ) as failures

,
        cast(
            'dummy_string' as varchar
        ) as query_id

,
        cast(
            'dummy_string' as varchar
        ) as thread_id

,
        cast(
            'dummy_string' as varchar
        ) as materialization

,
        cast(
            'dummy_string' as varchar
        ) as adapter_response

,
        cast(
            'dummy_string' as varchar
        ) as group_name


        ) as empty_table
        where 1 = 0