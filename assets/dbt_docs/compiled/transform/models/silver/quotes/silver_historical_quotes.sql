




with
    
        watermark as (
            select
                COALESCE(
                    MAX(silver_updated_at),
                    CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
                ) as max_time
            from "lakehouse_main"."silver"."silver_historical_quotes"
        ),
    

    new_data as (
        select *
        from "lakehouse_main"."staging"."staging_historical_quotes"
        
            where staged_at > (select max_time from watermark)
        
    ),

    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, date order by bronze_ingested_time DESC
            ) as rn
        from new_data
    ),

    applied_dq_rules as (
        select
            *, 

    

    
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. AUTO-CHECK NULLS FOR MANDATORY COLUMNS (price_basic,
                -- price_close, ticker...)
                
                    
                        case
                            when price_basic is NULL
                            then 'price_basic is mandatory but null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when price_close is NULL
                            then 'price_close is mandatory but null'
                            else NULL
                        end,
                    
                
                    
                
                    
                

                -- 2. AUTO-CHECK FOR NON-NEGATIVE VALUES (prices, volume)
                
                    
                        case
                            when price_basic < 0
                            then 'price_basic cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when price_open < 0
                            then 'price_open cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when price_high < 0
                            then 'price_high cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when price_low < 0
                            then 'price_low cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when price_close < 0
                            then 'price_close cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when total_volume < 0
                            then 'total_volume cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when total_value < 0
                            then 'total_value cannot be negative'
                            else NULL
                        end,
                    
                
                NULL
            ),
            ''
        )
    

 as unqualified_reason
        from deduped_data
        where rn = 1
    )

select
    ticker,
    date,
    EXTRACT(YEAR from date) as year,
    EXTRACT(MONTH from date) as month,
    EXTRACT(QUARTER from date) as quarter,
    (EXTRACT(YEAR from date) * 12 + EXTRACT(MONTH from date)) as absolute_month,
    (EXTRACT(YEAR from date) * 4 + EXTRACT(QUARTER from date)) as absolute_quarter,

    
        COALESCE(price_basic, 0) as price_basic,
    
        COALESCE(price_open, 0) as price_open,
    
        COALESCE(price_high, 0) as price_high,
    
        COALESCE(price_low, 0) as price_low,
    
        COALESCE(price_close, 0) as price_close,
    
        COALESCE(total_volume, 0) as total_volume,
    
        COALESCE(total_value, 0) as total_value,
    

     CAST(from_iso8601_timestamp('2026-05-06T08:55:22.931753+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,  '273468de-8a49-4a91-9bc2-2aabb801915e' as silver_invocation_id, 

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    unqualified_reason

from applied_dq_rules