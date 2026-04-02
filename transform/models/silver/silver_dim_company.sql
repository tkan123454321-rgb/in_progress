{{ config(
    materialized='table',
    unique_key='ticker'
) }}

{% set audit_cols = get_audit_columns('silver') %}

WITH cleaned_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker 
            ORDER BY bronze_ingested_time DESC 
        ) as rn
    FROM {{ ref('staging_dim_company') }}
    WHERE 
        ticker IS NOT NULL
        AND regexp_like(ticker, '^[A-Z0-9]{3}$')
        AND com_type_code IS NOT NULL
)

SELECT 
    ticker,
    COALESCE(company_name, 'Unknown Company') AS company_name,
    COALESCE(industry_group, 'Unclassified') AS industry_group,
    COALESCE(sector_detail, 'Unclassified') AS sector_detail,
    com_type_code as company_type,
    
    -- Vòng lặp đẻ cột Audit từ Dictionary
    {% for col in audit_cols %}
        {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

FROM cleaned_data
WHERE rn = 1

