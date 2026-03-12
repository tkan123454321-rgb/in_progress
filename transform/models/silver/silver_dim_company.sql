{{ config( materialized='table' ) }}

WITH cleaned_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker 
            ORDER BY inserted_bronze_time DESC ) as rn
    FROM {{ ref('raw_dim_company') }}
    WHERE 
        ticker IS NOT NULL
        AND regexp_like(ticker, '^[A-Z0-9]{3}$')
),
final AS (
    SELECT 
        ticker,
        COALESCE(company_name, 'Unknown Company') AS company_name,
        COALESCE(industry_group, 'Unclassified') AS industry_group,
        COALESCE(sector_detail, 'Unclassified') AS sector_detail,
        {{ generate_audit_columns('silver') }}
    FROM cleaned_data
    WHERE rn = 1
)

SELECT * FROM final
