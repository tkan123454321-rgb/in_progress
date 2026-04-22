{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['staging', 'fundamental_quarter'], 
    incremental_strategy='merge',
    unique_key=['ticker', 'year', 'quarter']
) }}

{% set indicators = get_financial_reports_column('fundamental_quarter') %}
{% set audit_cols = get_audit_columns('silver') %} 

WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, year, quarter
            ORDER BY bronze_ingested_time DESC
        ) as rn
    FROM {{ ref('staging_fundamental_quarter') }}

    {% if is_incremental() %}
      WHERE staged_at > (
          SELECT COALESCE(MAX(staged_at), CAST('1900-01-01 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)) 
          FROM {{ this }}
      )
    {% endif %}
),

applied_dq_rules AS (
    SELECT *,
        {{ dq_check_financial_reports('fundamental_quarter') }} AS unqualified_reason
    FROM deduped_bronze
    WHERE rn = 1
)


SELECT 
    ticker,
    year,
    quarter,
    
    -- Xử lý mượt mà: Có số thì lấy, API lười trả NULL thì ép về 0
    {% for ind in indicators %}
        COALESCE({{ ind.alias }}, 0) AS {{ ind.alias }},
    {% endfor %}

    -- Cột Audit
    {% for col in audit_cols %}
    {{ col.expr }} AS {{ col.alias }},
    {% endfor %}
    
    -- Đánh cờ trạng thái
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason

FROM applied_dq_rules
