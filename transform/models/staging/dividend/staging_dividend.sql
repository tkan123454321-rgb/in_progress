{{ config(
    materialized='table',
    tags=['staging', 'dividend']
) }}

{% set max_ingest_time = get_max_timestamp('bronze', 'dividend_year', 'bronze_ingested_time') %}
{% set fields = get_dividend_columns() %}
{% set audit_cols = get_audit_columns('staging') %}

WITH raw_source AS (
    SELECT * FROM {{ source('bronze', 'dividend_year') }}
    WHERE 
        bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' 
        >= 
        TRY_CAST('{{ max_ingest_time }}' AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' - INTERVAL '2' DAY
    AND year >= 2018
)

SELECT  
    TRIM(UPPER(ticker)) AS ticker,
    year,
    {% for field in fields %}
        TRY_CAST(json_extract_scalar(data, '$.{{ field.json_key }}') AS {{ field.type }}) AS {{ field.alias }},
    {% endfor %}
    
    {% for col in audit_cols %}
        {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
    
FROM raw_source
