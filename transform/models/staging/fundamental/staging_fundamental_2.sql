{{ config(materialized="table", tags=["staging", "fundamental"]) }}

{% set max_ingest_time = get_max_timestamp(
    "bronze", "fundamental_2", "bronze_ingested_time"
) %}
{% set fields = get_fundamental_columns("fundamental_2") %}
{% set audit_cols = get_audit_columns("staging") %}

with
    raw_source as (
        select *
        from {{ source("bronze", "fundamental_2") }}
        where
            bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' >= TRY_CAST(
                '{{ max_ingest_time }}' as TIMESTAMP with TIME ZONE
            ) AT TIME ZONE 'Asia/Ho_Chi_Minh'
            - interval '2' DAY
    )

select
    TRIM(UPPER(ticker)) as ticker,

    -- 1. Bóc tách JSON
    {% for field in fields %}
        TRY_CAST(
            json_extract_scalar(data, '$.{{ field.json_key }}') as {{ field.type }}
        ) as {{ field.alias }},
    {% endfor %}

    -- 2. Đẻ cột Audit
    {% for col in audit_cols %}
        {{ col.expr }} as {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

from raw_source
