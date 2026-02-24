{{ config( materialized='table' ) }}

WITH cleaned_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker 
            ORDER BY ingested_at DESC ) as rn
    FROM {{ ref('raw_dim_company') }}
    WHERE 
        ticker IS NOT NULL
        AND regexp_like(ticker, '^[A-Z0-9]{3}$')
),
final AS (
    SELECT 
        company_sk,
        ticker,
        COALESCE(company_name, 'Unknown Company') AS company_name,
        COALESCE(industry_group, 'Unclassified') AS industry_group,
        COALESCE(sector_detail, 'Unclassified') AS sector_detail,
        ingested_at,
        staged_at,
        from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AT TIME ZONE 'Asia/Ho_Chi_Minh' AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_silver_invocation_id
    FROM cleaned_data
    WHERE rn = 1
)

SELECT * FROM final
