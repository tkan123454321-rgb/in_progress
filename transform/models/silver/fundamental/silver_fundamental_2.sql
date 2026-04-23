{{ config(
    materialized='table'
    ) }}

{% set fields = get_fundamental_columns('fundamental_2') %}
{% set audit_cols = get_audit_columns('silver') %}

with deduped_data as (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker 
            ORDER BY bronze_ingested_time DESC ) as rn
    FROM {{ ref('staging_fundamental_2') }} ),

applied_dq_rules AS (
    SELECT *,
            {{ check_fundamental_columns('fundamental_2') }} AS unqualified_reason

    FROM deduped_data  
    WHERE rn = 1
)

SELECT 
    ticker,
    {% for field in fields %}
    {{ field.alias }},
    {% endfor %}
    
    unqualified_reason,

    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    
    {% for col in audit_cols %}
    {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
    
FROM applied_dq_rules