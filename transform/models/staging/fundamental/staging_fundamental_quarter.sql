{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['staging', 'quarter'], 
    incremental_strategy='append'
) }}

{% set indicators = get_fundamental_column('fundamental_quarter') %}
{% set audit_cols = get_audit_columns('staging') %} 

with raw_source as (
    select * from {{ source('bronze', 'fundamental_quarter') }}
    {% if is_incremental() %}
        where bronze_ingested_time > (
            SELECT COALESCE(MAX(bronze_ingested_time), CAST('1900-01-01 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)) 
            FROM {{ this }}
        ) AND year >= 2018
    {% endif %}
)
SELECT 
    TRY_CAST(ticker AS VARCHAR) AS ticker,
    TRY_CAST(year AS INT) AS year,
    TRY_CAST(quarter AS INT) AS quarter,
    {% for indicator in indicators %}
        TRY_CAST(json_extract_scalar(value, '$.{{ indicator.json_key }}') AS {{ indicator.type }}) AS {{ indicator.alias }},
    {% endfor %}
    {% for col in audit_cols %}
        {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
from raw_source


