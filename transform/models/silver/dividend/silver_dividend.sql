{{ config(
    materialized='table',
    tags=['silver', 'dividend']
) }}

{% set indicators = get_dividend_columns() %}
{% set audit_cols = get_audit_columns('silver') %} 

WITH deduped_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, year
            ORDER BY bronze_ingested_time DESC
        ) as rn
    FROM {{ ref('staging_dividend') }}
),

applied_dq_rules AS (
    SELECT *,
        {{ check_dividend_columns() }} AS unqualified_reason
    FROM deduped_data
    WHERE rn = 1
)

SELECT 
    ticker,
    year,

    {% for ind in indicators %}
        COALESCE({{ ind.alias }}, 0) AS {{ ind.alias }},
    {% endfor %}

    {% for col in audit_cols %}
    {{ col.expr }} AS {{ col.alias }},
    {% endfor %}
    
    CASE 
        WHEN unqualified_reason IS NULL OR unqualified_reason = '' THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    
    unqualified_reason

FROM applied_dq_rules
