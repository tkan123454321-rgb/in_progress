{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['silver', 'income_statement_quarter'], 
    unique_key=['ticker', 'year', 'quarter'],
    incremental_strategy='merge'
) }}

{% set indicators = get_financial_reports_column('income_statement') %}
{% set audit_cols = get_audit_columns('silver') %} 
{% set is_pivot = true %}

-- BƯỚC 1: LỌC TRÙNG VÀ INCREMENTAL TRỰC TIẾP TỪ BRONZE
WITH deduped_bronze AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, year, quarter, indicator_id 
            ORDER BY bronze_ingested_time DESC
        ) as rn
    FROM {{ source('bronze', 'financial_reports_quarter') }}
    WHERE data_type = 'income_statement_quarter' 
      AND year >= 2018

    {% if is_incremental() %}
      AND bronze_ingested_time > (
          SELECT COALESCE(MAX(bronze_ingested_time), CAST('1900-01-01 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)) 
          FROM {{ this }}
      )
    {% endif %}
),

-- BƯỚC 2: PIVOT TỪ TẬP DỮ LIỆU ĐÃ SẠCH (rn = 1)
pivoted_data AS (
    SELECT
        ticker,
        year,
        quarter,
        
        
    -- 1. Xoay cột (Pivot) các chỉ số tài chính
    {% for ind in indicators %}
        MAX(CASE WHEN indicator_id = {{ ind.id }} THEN CAST(value AS {{ ind.type }}) END) AS {{ ind.alias }},
    {% endfor %}

    -- 2. Đẻ cột Audit (Trở về vòng lặp cơ bản)
    {% for col in audit_cols %}
        {% if not col.is_from_staging %}
        
            {% if is_pivot and col.needs_agg %}
                MAX({{ col.expr }}) AS {{ col.alias }}
            {% else %}
                {{ col.expr }} AS {{ col.alias }}
            {% endif %}
            
            {%- if not loop.last %},{% endif -%}   
            
        {% endif %}
    {% endfor %}

    FROM deduped_bronze
    WHERE rn = 1 
    GROUP BY ticker, year, quarter
),
--bước 3: Áp dụng quy tắc DQ để tạo cột unqualified_reason
applied_dq_rules AS (
    SELECT *,
        NULLIF(
            CONCAT_WS(', ',
                {% for ind in indicators %}
                    CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is null' END
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            ), 
            '' -- THÊM DÒNG NÀY ĐỂ NULLIF HOẠT ĐỘNG
        ) AS unqualified_reason
    FROM pivoted_data
)
SELECT 
    ticker,
    year,
    quarter,
    
    {% for ind in indicators %}
        {{ ind.alias }},
    {% endfor %}

    {% for col in audit_cols if not col.is_from_staging %}
        {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %},
    
    CASE 
        WHEN unqualified_reason IS NOT NULL THEN 'unqualified' 
        ELSE 'qualified' 
    END AS status,
    unqualified_reason
FROM applied_dq_rules