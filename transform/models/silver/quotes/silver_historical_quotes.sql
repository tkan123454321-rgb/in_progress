{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns",
        tags=["silver", "historical_quotes"],
        incremental_strategy="merge",
        unique_key=["ticker", "date"],
    )
}}

{% set indicators = get_financial_reports_column("historical_quotes") %}
{% set audit_cols = get_audit_columns("silver") %}

with
    {% if is_incremental() %}
        watermark as (
            select
                COALESCE(
                    MAX(silver_updated_at),
                    CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
                ) as max_time
            from {{ this }}
        ),
    {% endif %}

    new_data as (
        select *
        from {{ ref("staging_historical_quotes") }}
        {% if is_incremental() %}
            where staged_at > (select max_time from watermark)
        {% endif %}
    ),

    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, date order by bronze_ingested_time DESC
            ) as rn
        from new_data
    ),

    applied_dq_rules as (
        select
            *, {{ check_financial_reports("historical_quotes") }} as unqualified_reason
        from deduped_data
        where rn = 1
    )

select
    ticker,
    date,
    EXTRACT(YEAR from date) as year,
    EXTRACT(MONTH from date) as month,
    EXTRACT(QUARTER from date) as quarter,
    (EXTRACT(YEAR from date) * 12 + EXTRACT(MONTH from date)) as absolute_month,
    (EXTRACT(YEAR from date) * 4 + EXTRACT(QUARTER from date)) as absolute_quarter,

    {% for ind in indicators %}
        COALESCE({{ ind.alias }}, 0) as {{ ind.alias }},
    {% endfor %}

    {% for col in audit_cols %} {{ col.expr }} as {{ col.alias }}, {% endfor %}

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    unqualified_reason

from applied_dq_rules
