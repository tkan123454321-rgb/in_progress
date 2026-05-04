{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns",
        tags=["staging", "historical_quotes"],
        incremental_strategy="append",
    )
}}

{% set indicators = get_financial_reports_column("historical_quotes") %}
{% set audit_cols = get_audit_columns("staging") %}

with
    raw_source as (
        select *
        from {{ source("bronze", "historical_quotes") }}
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
    TRY_CAST(event_date as DATE) as date,
    {% for indicator in indicators %}
        TRY_CAST(
            json_extract_scalar(
                data, '$.{{ indicator.json_key }}'
            ) as {{ indicator.type }}
        ) as {{ indicator.alias }},
    {% endfor %}
    {% for col in audit_cols %}
        {{ col.expr }} as {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
from raw_source
