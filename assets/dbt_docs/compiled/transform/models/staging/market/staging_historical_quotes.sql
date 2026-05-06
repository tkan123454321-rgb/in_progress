




with
    
        watermark as (
            select
                COALESCE(
                    MAX(bronze_ingested_time),
                    CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
                ) as max_time
            from "lakehouse_main"."staging"."staging_historical_quotes"
        ),
    

    raw_source as (
        select *
        from "lakehouse_main"."bronze"."historical_quotes"
        
            where
                bronze_ingested_time > (select max_time from watermark) and year >= 2018
        
    )
select
    TRY_CAST(ticker as VARCHAR) as ticker,
    TRY_CAST(event_date as DATE) as date,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.priceBasic'
            ) as DECIMAL(20,4)
        ) as price_basic,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.priceOpen'
            ) as DECIMAL(20,4)
        ) as price_open,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.priceHigh'
            ) as DECIMAL(20,4)
        ) as price_high,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.priceLow'
            ) as DECIMAL(20,4)
        ) as price_low,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.priceClose'
            ) as DECIMAL(20,4)
        ) as price_close,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.totalVolume'
            ) as DOUBLE
        ) as total_volume,
    
        TRY_CAST(
            json_extract_scalar(
                data, '$.totalValue'
            ) as DOUBLE
        ) as total_value,
    
    
        bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' as bronze_ingested_time,
    
        CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as staged_at,
    
        '4ff423e7-7675-4eec-a090-58bdf9560b12' as staging_invocation_id
    
from raw_source