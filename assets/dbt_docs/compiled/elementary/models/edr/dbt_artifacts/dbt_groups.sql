

select * from (
            select
        cast(
            'dummy_string' as varchar
        ) as unique_id

,
        cast(
            'dummy_string' as varchar
        ) as name

,
        cast(
            'dummy_string' as varchar
        ) as owner_email

,
        cast(
            'dummy_string' as varchar
        ) as owner_name

,
        cast(
            'dummy_string' as varchar
        ) as generated_at

,
        cast(
            'dummy_string' as varchar
        ) as metadata_hash


        ) as empty_table
        where 1 = 0