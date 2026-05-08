




with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker order by bronze_ingested_time DESC
            ) as rn
        from "lakehouse_main"."staging"."staging_fundamental_2"
    ),

    applied_dq_rules as (
        select *, 

    

    
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. Rule: Check for valid stock exchanges
                case
                    when UPPER(exchange) not in ('UPCOM', 'HSX', 'HOSE', 'HNX')
                    then 'invalid_exchange'
                    else NULL
                end,

                -- 2. Rule: Check listing status (Must be actively listed)
                case when is_listing = FALSE then 'delisted_or_inactive' else NULL end,

                -- 3. Check for NULLs in mandatory columns
                
                    
                        case
                            when exchange is NULL
                            then 'exchange is mandatory but null'
                            else NULL
                        end,
                    
                
                    
                        case
                            when is_listing is NULL
                            then 'is_listing is mandatory but null'
                            else NULL
                        end,
                    
                

                NULL  -- Trick to prevent trailing pipe errors
            ),
            ''
        )
    

 as unqualified_reason

        from deduped_data
        where rn = 1
    )

select
    ticker,
     exchange,  is_listing, 

    unqualified_reason,

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    
        CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,
    
        '4ff423e7-7675-4eec-a090-58bdf9560b12' as silver_invocation_id
    

from applied_dq_rules