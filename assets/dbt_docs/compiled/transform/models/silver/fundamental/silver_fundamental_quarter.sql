




-- STEP 1: DEDUPLICATE STAGING DATA
-- Retrieve the latest record for each ticker, year, and quarter based
-- on ingestion time.
with
    watermark as (
        select
            COALESCE(
                MAX(silver_updated_at),
                CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
            ) as max_time
        from "lakehouse_main"."silver"."silver_fundamental_quarter"
    ),

    new_data as (
        select *
        from "lakehouse_main"."staging"."staging_fundamental_quarter"
        
            where staged_at > (select max_time from watermark)
        
    ),

    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, year, quarter order by bronze_ingested_time DESC
            ) as rn
        from new_data
    ),

    applied_dq_rules as (
        select
            *,
            

    

    
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. AUTO-CHECK NULLS FOR MANDATORY COLUMNS (is_mandatory)
                
                    
                
                    
                        case
                            when market_cap is NULL
                            then 'market_cap is mandatory but null'
                            else NULL
                        end,
                    
                
                    
                        case
                            when shares_outstanding is NULL
                            then 'shares_outstanding is mandatory but null'
                            else NULL
                        end,
                    
                

                -- 2. AUTO-CHECK FOR NON-NEGATIVE VALUES (must_be_positive)
                
                    
                
                    
                        case
                            when market_cap < 0
                            then 'market_cap cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when shares_outstanding < 0
                            then 'shares_outstanding cannot be negative'
                            else NULL
                        end,
                    
                

                -- 3. CUSTOM BUSINESS LOGIC CHECKS (If needed)
                -- Example: Market Cap must be greater than 0
                case when market_cap = 0 then 'Err: Market Cap is zero' else NULL end
            ),
            ''
        )
    

 as unqualified_reason
        from deduped_data
        where rn = 1
    )

select
    ticker,
    year,
    quarter,

    
        COALESCE(preferred_stock, 0) as preferred_stock,
    
        COALESCE(market_cap, 0) as market_cap,
    
        COALESCE(shares_outstanding, 0) as shares_outstanding,
    

     CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,  '4c6d9271-375a-4d96-926e-49714c96b216' as silver_invocation_id, 

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    unqualified_reason

from applied_dq_rules