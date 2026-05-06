





with
    raw_source as (
        select *
        from "lakehouse_main"."bronze"."fundamental_1"
        where
            bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' >= TRY_CAST(
                '2026-05-05 04:57:25.808059+00:00' as TIMESTAMP with TIME ZONE
            ) AT TIME ZONE 'Asia/Ho_Chi_Minh'
            - interval '2' DAY
    )

select
    TRIM(UPPER(ticker)) as ticker,

    -- 1. JSON parsing
    
        TRY_CAST(
            json_extract_scalar(data, '$.sharesOutstanding') as DOUBLE
        ) as shares_outstanding,
    
        TRY_CAST(
            json_extract_scalar(data, '$.freeShares') as DOUBLE
        ) as floating_shares,
    
        TRY_CAST(
            json_extract_scalar(data, '$.marketCap') as DOUBLE
        ) as market_cap,
    
        TRY_CAST(
            json_extract_scalar(data, '$.avgVolume3m') as DOUBLE
        ) as avg_volume_3m,
    
        TRY_CAST(
            json_extract_scalar(data, '$.insiderOwnership') as DOUBLE
        ) as insider_ownership,
    
        TRY_CAST(
            json_extract_scalar(data, '$.institutionOwnership') as DOUBLE
        ) as institution_ownership,
    
        TRY_CAST(
            json_extract_scalar(data, '$.foreignOwnership') as DOUBLE
        ) as foreign_ownership,
    

    -- 2. Audit columns
    
        bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' as bronze_ingested_time,
    
        CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as staged_at,
    
        '4c6d9271-375a-4d96-926e-49714c96b216' as staging_invocation_id
    

from raw_source