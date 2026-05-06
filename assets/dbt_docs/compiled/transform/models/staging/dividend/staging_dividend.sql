with
    raw_source as (
        select *
        from "lakehouse_main"."bronze"."dividend_year"
        where
            bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' >= TRY_CAST(
                '2026-05-05 05:49:27.574767+00:00' as TIMESTAMP with TIME ZONE
            ) AT TIME ZONE 'Asia/Ho_Chi_Minh'
            - interval '2' DAY
            and year >= 2018
    )

select
    TRIM(UPPER(ticker)) as ticker,
    year,

    TRY_CAST(json_extract_scalar(data, '$.cashDividend') as DOUBLE) as cash_dividend,

    TRY_CAST(json_extract_scalar(data, '$.stockDividend') as DOUBLE) as stock_dividend,

    bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' as bronze_ingested_time,

    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as staged_at,

    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as staging_invocation_id

from raw_source
