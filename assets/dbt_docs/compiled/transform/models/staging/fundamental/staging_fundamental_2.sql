with
    raw_source as (
        select *
        from "lakehouse_main"."bronze"."fundamental_2"
        where
            bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' >= TRY_CAST(
                '2026-05-05 05:01:39.815030+00:00' as TIMESTAMP with TIME ZONE
            ) AT TIME ZONE 'Asia/Ho_Chi_Minh'
            - interval '2' DAY
    )

select
    TRIM(UPPER(ticker)) as ticker,

    -- 1. JSON parsing
    TRY_CAST(json_extract_scalar(data, '$.exchange') as VARCHAR) as exchange,

    TRY_CAST(json_extract_scalar(data, '$.isListing') as BOOLEAN) as is_listing,

    -- 2. Audit columns
    bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' as bronze_ingested_time,

    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as staged_at,

    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as staging_invocation_id

from raw_source
