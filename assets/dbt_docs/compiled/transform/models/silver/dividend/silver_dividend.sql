




with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, year order by bronze_ingested_time DESC
            ) as rn
        from "lakehouse_main"."staging"."staging_dividend"
    ),

    applied_dq_rules as (
        select *, 

    

    NULLIF(
        CONCAT_WS(
            ' | ',

            -- AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
            
                
                    case
                        when COALESCE(cash_dividend, 0) < 0
                        then 'cash_dividend cannot be negative'
                        else NULL
                    end,
                
            
                
                    case
                        when COALESCE(stock_dividend, 0) < 0
                        then 'stock_dividend cannot be negative'
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
    year,

    
        COALESCE(cash_dividend, 0) as cash_dividend,
    
        COALESCE(stock_dividend, 0) as stock_dividend,
    

     CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,  '4c6d9271-375a-4d96-926e-49714c96b216' as silver_invocation_id, 

    case
        when unqualified_reason is NULL or unqualified_reason = ''
        then 'qualified'
        else 'unqualified'
    end as status,

    unqualified_reason

from applied_dq_rules