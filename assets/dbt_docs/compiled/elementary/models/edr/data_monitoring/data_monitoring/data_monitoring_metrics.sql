


    
    
        
    
    select * from (
            select
        cast(
            'dummy_string' as varchar
        ) as id

,
        cast(
            'dummy_string' as varchar
        ) as full_table_name

,
        cast(
            'dummy_string' as varchar
        ) as column_name

,
        cast(
            'dummy_string' as varchar
        ) as metric_name

,
        cast(
            'dummy_string' as varchar
        ) as metric_type

,
        cast(
            123456789.99 as double
        ) as metric_value

,
        cast(
            'dummy_string' as varchar
        ) as source_value

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as bucket_start

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as bucket_end

,
        cast(
            123456789 as integer
        ) as bucket_duration_hours

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as updated_at

,
        cast(
            'dummy_string' as varchar
        ) as dimension

,
        cast(
            'dummy_string' as varchar
        ) as dimension_value

,
        cast(
            'dummy_string' as varchar
        ) as metric_properties

,cast(
            '2091-02-17' as  timestamp(6) 
        ) as created_at


        ) as empty_table
        where 1 = 0
