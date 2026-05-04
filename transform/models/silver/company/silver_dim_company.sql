{{ config(materialized="table", tags=["silver", "dim_company"]) }}
{% set audit_cols = get_audit_columns("silver") %}

with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker
                order by company_name ASC, industry_group ASC, sector_detail ASC
            ) as rn
        from {{ ref("bronze_dim_company") }}
        where
            ticker is not NULL
            and regexp_like(ticker, '^[A-Z0-9]{3}$')
            and company_type is not NULL
    )
select
    ticker,
    COALESCE(company_name, 'Unknown Company') as company_name,
    COALESCE(industry_group, 'Unclassified') as industry_group,
    COALESCE(sector_detail, 'Unclassified') as sector_detail,
    company_type
    {% for col in audit_cols %},{{ col.expr }} as {{ col.alias }} {% endfor %}
from deduped_data
where rn = 1
