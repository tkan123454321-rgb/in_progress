{{ config(
    unique_key='ticker'
) }}

WITH raw_source AS (
    SELECT symbol,
        organ_name,
        icb_name2,
        icb_name4,
        ingestion_time
    FROM {{ source('bronze', 'raw_companies_listing') }}
),
staged_data as (
    SELECT 
        TRIM(UPPER(symbol)) AS ticker,
        NULLIF(TRIM(organ_name), '') AS company_name,
        NULLIF(TRIM(icb_name2), '') AS industry_group,
        NULLIF(TRIM(icb_name4), '') AS sector_detail,
        TRY_CAST(ingestion_time AS TIMESTAMP) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS inserted_bronze_time,
        from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AT TIME ZONE 'Asia/Ho_Chi_Minh' AS staged_at,
        '{{ invocation_id }}' AS staging_invocation_id
        from raw_source
 ) 
SELECT * FROM staged_data