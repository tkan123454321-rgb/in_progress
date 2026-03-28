{{ config(
    materialized='table'
    ) }}

{% set fields = get_fundamental_columns('fundamental_2') %}

with deduped_data as (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker 
            ORDER BY bronze_ingested_time DESC ) as rn
    FROM {{ ref('staging_fundamental_2') }} ),

applied_dq_rules AS (
    SELECT *,
            NULLIF(
                    CONCAT_WS(', ',
                        -- 1. Luật kiểm tra sàn giao dịch (Major exchanges only)
                        CASE 
                            WHEN UPPER(exchange) NOT IN ('UPCOM', 'HSX', 'HOSE', 'HNX') 
                            THEN 'invalid_exchange' 
                        END,

                        -- 2. Luật kiểm tra trạng thái niêm yết (Must be actively listed)
                        CASE 
                            WHEN is_listing = FALSE 
                            THEN 'delisted_or_inactive' 
                        END,

                        -- 3. Kế thừa luật check NULL và số Âm từ Macro
                        {% for field in fields %}
                        CASE 
                            {% if field.is_mandatory %}
                            WHEN {{ field.alias }} IS NULL THEN '{{ field.alias }} is null'
                            {% endif %}
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
    unqualified_reason,
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    {{ generate_audit_columns('silver') }}
FROM applied_dq_rules