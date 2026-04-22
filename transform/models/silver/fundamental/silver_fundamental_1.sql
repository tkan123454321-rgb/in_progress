{{ config(
    materialized='table'
    ) }}

{% set fields = get_fundamental_columns('fundamental_1') %}
{% set audit_cols = get_audit_columns('silver') %}

with deduped_data as (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker 
            ORDER BY bronze_ingested_time DESC ) as rn
    FROM {{ ref('staging_fundamental_1') }} ),

applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(', ',
                {% for field in fields %}
                CASE 
                    -- Nếu Macro đánh dấu là bắt buộc (True), thì nôn ra dòng check NULL này:
                    {% if field.is_mandatory %}
                    WHEN {{ field.alias }} IS NULL THEN '{{ field.alias }} is null'
                    {% endif %}
                    
                    -- Dòng check âm thì áp dụng cho tất cả:
                    WHEN {{ field.alias }} < 0 THEN '{{ field.alias }} < 0' 
                END{% if not loop.last %},{% endif %}
                {% endfor %}
            ),
            ''
        ) AS unqualified_reason

    FROM deduped_data  
    WHERE rn = 1
)

SELECT 
    ticker,
    {% for field in fields %}
    {{ field.alias }},
    {% endfor %}

    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason,
    
    {% for col in audit_cols %}
    {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
    
FROM applied_dq_rules



