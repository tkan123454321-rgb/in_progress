{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['silver', 'historical_quotes'], 
    incremental_strategy='merge',
    unique_key=['ticker', 'date']
) }}

{% set indicators = get_financial_reports_column('historical_quotes') %}
{% set audit_cols = get_audit_columns('silver') %}

WITH deduped_staging AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, date -- Lọc trùng theo mã và ngày
            ORDER BY bronze_ingested_time DESC
        ) as rn
    FROM {{ ref('staging_historical_quotes') }} -- Trỏ vào bảng staging vừa làm

    {% if is_incremental() %}
      WHERE staged_at > (
          SELECT COALESCE(MAX(staged_at), CAST('1900-01-01 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)) 
          FROM {{ this }}
      )
    {% endif %}
),

applied_dq_rules AS (
    SELECT *,
        {{ dq_check_financial_reports('historical_quotes') }} AS unqualified_reason
    FROM deduped_staging
    WHERE rn = 1
)
SELECT 
    ticker,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(QUARTER FROM date) AS quarter,
    
    -- Absolute IDs để phục vụ LAG/LEAD mượt mà ở các tầng sau
    (EXTRACT(YEAR FROM date) * 12 + EXTRACT(MONTH FROM date)) AS absolute_month,
    (EXTRACT(YEAR FROM date) * 4 + EXTRACT(QUARTER FROM date)) AS absolute_quarter,
    
    -- Xử lý mượt mà: Có số thì lấy, API lười trả NULL thì ép về 0
    {% for ind in indicators %}
        COALESCE({{ ind.alias }}, 0) AS {{ ind.alias }},
    {% endfor %}

    {% for col in audit_cols %}
    {{ col.expr }} AS {{ col.alias }},
    {% endfor %}
    
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason

FROM applied_dq_rules