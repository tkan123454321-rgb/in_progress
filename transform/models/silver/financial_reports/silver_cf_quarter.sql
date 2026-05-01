{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['silver', 'cash_flow_indirect_quarter', 'quarter'], 
    unique_key=['ticker', 'year', 'quarter'],
    incremental_strategy='merge'
) }}

{% set indicators = get_financial_reports_column('cash_flow_indirect') %}
{% set audit_cols = get_audit_columns('silver') %} 


-- STEP 1: DEDUPLICATE BRONZE DATA
-- Retrieve the latest record for each ticker, year, quarter, and indicator_id based on ingestion time.
WITH watermark AS (
    SELECT 
        COALESCE(MAX(silver_updated_at), CAST('1900-01-01 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)) as max_time
    FROM {{ this }}
),

new_data AS (
    SELECT *
    FROM {{ source('bronze', 'financial_reports_quarter') }}
    WHERE year >= 2018 
      AND data_type IN ('cash_flow_indirect_quarter')
    {% if is_incremental() %}
      AND bronze_ingested_time > (SELECT max_time FROM watermark)
    {% endif %}
),

deduped_data AS (
    SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, year, quarter, indicator_id 
                ORDER BY bronze_ingested_time DESC 
            ) as rn
    FROM new_data
),
-- STEP 2: PIVOT INDICATORS
-- Transform the data from long format (rows) to wide format (columns).
pivoted_data AS (
    SELECT
        ticker,
        year,
        quarter
        
    {% for ind in indicators %}
       ,MAX(CASE WHEN indicator_id = {{ ind.id }} THEN CAST(value AS {{ ind.type }}) END) AS {{ ind.alias }}
    {% endfor %}

    FROM deduped_data
    WHERE rn = 1 
    GROUP BY ticker, year, quarter
),
-- STEP 3: APPLY DATA QUALITY RULES
-- Evaluate data against predefined Data Quality rules to capture the unqualified reason.
applied_dq_rules AS (
    SELECT *,
        {{ check_financial_reports('cash_flow_indirect') }} AS unqualified_reason
    FROM pivoted_data
)

-- STEP 4: FINAL SELECTION & FORMATTING
-- Handle null values, append system audit columns, and determine the final DQ status.

SELECT 
    ticker,
    year,
    quarter,
    
    -- Replace NULL values with 0 for all financial indicators
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

    
    
                           
 
