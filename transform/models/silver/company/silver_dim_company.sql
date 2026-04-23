{{ config(
    materialized='table',
    tags=['silver', 'dim_company']
) }}
{% set audit_cols = get_audit_columns('silver') %}

WITH deduped_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY company_name ASC,
                industry_group ASC,
                sector_detail ASC
        ) as rn
    FROM {{ ref('bronze_dim_company') }}
    WHERE ticker IS NOT NULL
        AND regexp_like(ticker, '^[A-Z0-9]{3}$')
        AND company_type IS NOT NULL
)
SELECT ticker,
    COALESCE(company_name, 'Unknown Company') AS company_name,
    COALESCE(industry_group, 'Unclassified') AS industry_group,
    COALESCE(sector_detail, 'Unclassified') AS sector_detail,
    company_type
    {% for col in audit_cols %}
    {% if not col.is_from_staging %},
    {{ col.expr }} AS {{ col.alias }}
    {% endif %}
    {% endfor %}
FROM deduped_data
WHERE rn = 1