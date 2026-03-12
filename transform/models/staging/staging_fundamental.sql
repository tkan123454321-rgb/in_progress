{{ config(materialized='table') }}

{% set max_ingest_time = get_max_timestamp('bronze', 'fundamental', 'inserted_time') %}

{% set fields = get_fundamental_columns() %}



WITH raw_source AS (
    SELECT * FROM {{ source('bronze', 'fundamental') }}
    WHERE 
        -- Phòng thủ nhiều lớp (dù Python đã chặn)
        data IS NOT NULL 
        AND data != '{}'
        
        -- 2. NHÉT BIẾN VÀO ĐÂY: Dùng Jinja bọc biến max_ingest_time lại
        AND TRY_CAST(inserted_time AS TIMESTAMP) AT TIME ZONE 'Asia/Ho_Chi_Minh' 
        >= 
        TRY_CAST('{{ max_ingest_time }}' AS TIMESTAMP) AT TIME ZONE 'Asia/Ho_Chi_Minh' - INTERVAL '2' DAY
),
flatten_data as  (
    SELECT  
        message_id as kafka_message_id,
        {{ generate_audit_columns('staging') }}
        TRIM(UPPER(ticker)) AS ticker,
        {% for field in fields %}
            TRY_CAST(json_extract_scalar(data, '$.{{ field.json_key }}') AS {{ field.type }}) AS {{ field.alias }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM raw_source )


SELECT * FROM flatten_data
