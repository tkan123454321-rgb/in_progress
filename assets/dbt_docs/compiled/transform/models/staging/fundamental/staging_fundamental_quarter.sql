




with
    raw_source as (
        select *
        from "lakehouse_main"."bronze"."fundamental_quarter"

        
            where
                bronze_ingested_time > (
                    select
                        COALESCE(
                            MAX(bronze_ingested_time),
                            CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
                        )
                    from "lakehouse_main"."staging"."staging_fundamental_quarter"
                )
                and year >= 2018
        
    )
select
    TRY_CAST(ticker as VARCHAR) as ticker,
    TRY_CAST(year as INT) as year,
    TRY_CAST(quarter as INT) as quarter,
    
        TRY_CAST(
            json_extract_scalar(
                value, '$.PreferredStock'
            ) as DECIMAL(20,4)
        ) as preferred_stock,
    
        TRY_CAST(
            json_extract_scalar(
                value, '$.MarketCapAtPeriodEnd'
            ) as DECIMAL(20,4)
        ) as market_cap,
    
        TRY_CAST(
            json_extract_scalar(
                value, '$.ShareAtPeriodEnd'
            ) as DECIMAL(20,4)
        ) as shares_outstanding,
    
    
        bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' as bronze_ingested_time,
    
        CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as staged_at,
    
        '4ff423e7-7675-4eec-a090-58bdf9560b12' as staging_invocation_id
    
from raw_source