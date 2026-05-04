{{ config(materialized="table", tags=["silver", "dividend"]) }}

{% set indicators = get_dividend_columns() %}
{% set audit_cols = get_audit_columns("silver") %}

with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, year order by bronze_ingested_time DESC
            ) as rn
        from {{ ref("staging_dividend") }}
    ),

    applied_dq_rules as (
        select *, {{ check_dividend_columns() }} as unqualified_reason
        from deduped_data
        where rn = 1
    )

select
    ticker,
    year,

    {% for ind in indicators %}
        COALESCE({{ ind.alias }}, 0) as {{ ind.alias }},
    {% endfor %}

    {% for col in audit_cols %} {{ col.expr }} as {{ col.alias }}, {% endfor %}

    case
        when unqualified_reason is NULL or unqualified_reason = ''
        then 'qualified'
        else 'unqualified'
    end as status,

    unqualified_reason

from applied_dq_rules
