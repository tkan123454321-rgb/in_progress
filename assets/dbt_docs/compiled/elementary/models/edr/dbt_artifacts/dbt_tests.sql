select *
from
    (
        select
            cast('dummy_string' as varchar) as unique_id,
            cast('dummy_string' as varchar) as database_name,
            cast('dummy_string' as varchar) as schema_name,
            cast('dummy_string' as varchar) as name,
            cast('dummy_string' as varchar) as short_name,
            cast('dummy_string' as varchar) as alias,
            cast('dummy_string' as varchar) as test_column_name,
            cast('dummy_string' as varchar) as severity,
            cast('dummy_string' as varchar) as warn_if,
            cast('dummy_string' as varchar) as error_if,
            cast('this_is_just_a_long_dummy_string' as varchar) as test_params,
            cast('dummy_string' as varchar) as test_namespace,
            cast('dummy_string' as varchar) as test_original_name,
            cast('this_is_just_a_long_dummy_string' as varchar) as tags,
            cast('this_is_just_a_long_dummy_string' as varchar) as model_tags,
            cast('this_is_just_a_long_dummy_string' as varchar) as model_owners,
            cast('this_is_just_a_long_dummy_string' as varchar) as meta,
            cast('this_is_just_a_long_dummy_string' as varchar) as depends_on_macros,
            cast('this_is_just_a_long_dummy_string' as varchar) as depends_on_nodes,
            cast('dummy_string' as varchar) as parent_model_unique_id,
            cast('this_is_just_a_long_dummy_string' as varchar) as description,
            cast('dummy_string' as varchar) as package_name,
            cast('dummy_string' as varchar) as type,
            cast('this_is_just_a_long_dummy_string' as varchar) as original_path,
            cast('dummy_string' as varchar) as path,
            cast('dummy_string' as varchar) as generated_at,
            cast('dummy_string' as varchar) as metadata_hash,
            cast('dummy_string' as varchar) as quality_dimension,
            cast('dummy_string' as varchar) as group_name

    ) as empty_table
where 1 = 0
