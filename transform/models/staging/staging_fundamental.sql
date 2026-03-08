{{ config(materialized='table') }}

{% set max_ingest_time = get_max_column_value('bronze', 'fundamental', 'inserted_time') %}

{% set json_fields = [
    ('sharesOutstanding', 'DOUBLE', 'shares_outstanding'),
    ('freeShares', 'DOUBLE', 'floating_shares'),
    ('marketCap', 'DOUBLE', 'market_cap')
] %}



WITH raw_source AS (
    SELECT * FROM {{ source('bronze', 'fundamental') }}
    WHERE 
        -- Phòng thủ nhiều lớp (dù Python đã chặn)
        data IS NOT NULL 
        AND data != '{}'
        
        -- 2. NHÉT BIẾN VÀO ĐÂY: Dùng Jinja bọc biến max_ingest_time lại
        AND TIMESTAMP inserted_time AT TIME ZONE 'Asia/Ho_Chi_Minh' 
        >= 
        TIMESTAMP '{{ max_ingest_time }}' AT TIME ZONE 'Asia/Ho_Chi_Minh' - INTERVAL '2' DAY
),
flatten_data as  (
    SELECT  
        message_id as kafka_message_id,
        '{{ invocation_id }}' AS dbt_staging_invocation_id,
        TRY_CAST('{{ run_started_at.isoformat() }}' AS TIMESTAMP) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS staged_at,
        TRY_CAST(inserted_time AS TIMESTAMP) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS inserted_bronze_time,
        TRIM(UPPER(ticker)) AS ticker,
        {% for json_field, data_type, alias in json_fields %}
            TRY_CAST(json_extract_scalar(data, '$.{{ json_field }}') AS {{ data_type }}) AS {{ alias }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM raw_source
)