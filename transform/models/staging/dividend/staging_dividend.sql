{{ config(materialized="table", tags=["staging", "dividend"]) }}

{% set max_ingest_time = get_max_timestamp(
    "bronze", "dividend_year", "bronze_ingested_time"
) %}
{% set fields = get_dividend_columns() %}
{% set audit_cols = get_audit_columns("staging") %}

with
    raw_source as (
        select *
        from {{ source("bronze", "dividend_year") }}
        where
            bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' >= TRY_CAST(
                '{{ max_ingest_time }}' as TIMESTAMP with TIME ZONE
            ) AT TIME ZONE 'Asia/Ho_Chi_Minh'
            - interval '2' DAY
            and year >= 2018
    )

select
    TRIM(UPPER(ticker)) as ticker,
    year,
    {% for field in fields %}
        TRY_CAST(
            json_extract_scalar(data, '$.{{ field.json_key }}') as {{ field.type }}
        ) as {{ field.alias }},
    {% endfor %}

    {% for col in audit_cols %}
        {{ col.expr }} as {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

from raw_source
