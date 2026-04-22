{{ config(materialized='table') }}

{% set max_ingest_time = get_max_timestamp('bronze', 'fundamental_1', 'bronze_ingested_time') %}
{% set fields = get_fundamental_columns('fundamental_1') %}
{% set audit_cols = get_audit_columns('staging') %}

WITH raw_source AS (
    SELECT * FROM {{ source('bronze', 'fundamental_1') }}
    WHERE 
        bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' 
        >= 
        TRY_CAST('{{ max_ingest_time }}' AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' - INTERVAL '2' DAY
)

SELECT  
    TRIM(UPPER(ticker)) AS ticker,
    
    -- 1. Bóc tách JSON
    {% for field in fields %}
        TRY_CAST(json_extract_scalar(data, '$.{{ field.json_key }}') AS {{ field.type }}) AS {{ field.alias }},
    {% endfor %}
    
    -- 2. Đẻ cột Audit
    {% for col in audit_cols %}
        {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
    
FROM raw_source

