{{ config(materialized="table", tags=["silver", "fundamental"]) }}

{% set fields = get_fundamental_columns("fundamental_1") %}
{% set audit_cols = get_audit_columns("silver") %}

with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker order by bronze_ingested_time DESC
            ) as rn
        from {{ ref("staging_fundamental_1") }}
    ),

    applied_dq_rules as (
        select *, {{ check_fundamental_columns("fundamental_1") }} as unqualified_reason

        from deduped_data
        where rn = 1
    )

select
    ticker,
    {% for field in fields %} {{ field.alias }}, {% endfor %}

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason,

    {% for col in audit_cols %}
        {{ col.expr }} as {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

from applied_dq_rules
