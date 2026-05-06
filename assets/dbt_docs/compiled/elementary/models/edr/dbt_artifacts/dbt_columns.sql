select *
from
    (
        select
            cast('dummy_string' as varchar) as unique_id,
            cast('dummy_string' as varchar) as parent_unique_id,
            cast('dummy_string' as varchar) as name,
            cast('dummy_string' as varchar) as data_type,
            cast('this_is_just_a_long_dummy_string' as varchar) as tags,
            cast('this_is_just_a_long_dummy_string' as varchar) as meta,
            cast('dummy_string' as varchar) as database_name,
            cast('dummy_string' as varchar) as schema_name,
            cast('dummy_string' as varchar) as table_name,
            cast('this_is_just_a_long_dummy_string' as varchar) as description,
            cast('dummy_string' as varchar) as resource_type,
            cast('dummy_string' as varchar) as generated_at,
            cast('dummy_string' as varchar) as metadata_hash

    ) as empty_table
where 1 = 0
