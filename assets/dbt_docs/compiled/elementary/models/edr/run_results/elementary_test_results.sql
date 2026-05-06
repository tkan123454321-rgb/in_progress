select *
from
    (
        select
            cast('this_is_just_a_long_dummy_string' as varchar) as id,
            cast('dummy_string' as varchar) as data_issue_id,
            cast('this_is_just_a_long_dummy_string' as varchar) as test_execution_id,
            cast('this_is_just_a_long_dummy_string' as varchar) as test_unique_id,
            cast('this_is_just_a_long_dummy_string' as varchar) as model_unique_id,
            cast('dummy_string' as varchar) as invocation_id,
            cast('2091-02-17' as timestamp(6)) as detected_at,
            cast('2091-02-17' as timestamp(6)) as created_at,
            cast('dummy_string' as varchar) as database_name,
            cast('dummy_string' as varchar) as schema_name,
            cast('dummy_string' as varchar) as table_name,
            cast('dummy_string' as varchar) as column_name,
            cast('dummy_string' as varchar) as test_type,
            cast('dummy_string' as varchar) as test_sub_type,
            cast(
                'this_is_just_a_long_dummy_string' as varchar
            ) as test_results_description,
            cast('dummy_string' as varchar) as owners,
            cast('dummy_string' as varchar) as tags,
            cast('this_is_just_a_long_dummy_string' as varchar) as test_results_query,
            cast('dummy_string' as varchar) as other,
            cast('this_is_just_a_long_dummy_string' as varchar) as test_name,
            cast('this_is_just_a_long_dummy_string' as varchar) as test_params,
            cast('dummy_string' as varchar) as severity,
            cast('dummy_string' as varchar) as status,
            cast(31474836478 as bigint) as failures,
            cast('dummy_string' as varchar) as test_short_name,
            cast('dummy_string' as varchar) as test_alias,
            cast('this_is_just_a_long_dummy_string' as varchar) as result_rows,
            cast(31474836478 as bigint) as failed_row_count

    ) as empty_table
where 1 = 0
