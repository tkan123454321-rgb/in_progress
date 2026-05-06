select *
from
    (
        select
            cast('dummy_string' as varchar) as unique_id,
            cast('dummy_string' as varchar) as alias,
            cast('dummy_string' as varchar) as checksum,
            cast('this_is_just_a_long_dummy_string' as varchar) as tags,
            cast('this_is_just_a_long_dummy_string' as varchar) as meta,
            cast('dummy_string' as varchar) as owner,
            cast('dummy_string' as varchar) as database_name,
            cast('dummy_string' as varchar) as schema_name,
            cast('this_is_just_a_long_dummy_string' as varchar) as description,
            cast('dummy_string' as varchar) as name,
            cast('dummy_string' as varchar) as package_name,
            cast('this_is_just_a_long_dummy_string' as varchar) as original_path,
            cast('dummy_string' as varchar) as path,
            cast('dummy_string' as varchar) as generated_at,
            cast('dummy_string' as varchar) as metadata_hash,
            cast('dummy_string' as varchar) as group_name

    ) as empty_table
where 1 = 0
