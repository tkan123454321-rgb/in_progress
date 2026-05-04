{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns",
        tags=["staging", "quarter"],
        incremental_strategy="append",
    )
}}

{% set indicators = get_fundamental_columns("fundamental_quarter") %}
{% set audit_cols = get_audit_columns("staging") %}

with
    raw_source as (
        select *
        from {{ source("bronze", "fundamental_quarter") }}

        {% if is_incremental() %}
            where
                bronze_ingested_time > (
                    select
                        COALESCE(
                            MAX(bronze_ingested_time),
                            CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
                        )
                    from {{ this }}
                )
                and year >= 2018
        {% endif %}
    )
select
    TRY_CAST(ticker as VARCHAR) as ticker,
    TRY_CAST(year as INT) as year,
    TRY_CAST(quarter as INT) as quarter,
    {% for indicator in indicators %}
        TRY_CAST(
            json_extract_scalar(
                value, '$.{{ indicator.json_key }}'
            ) as {{ indicator.type }}
        ) as {{ indicator.alias }},
    {% endfor %}
    {% for col in audit_cols %}
        {{ col.expr }} as {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
from raw_source
