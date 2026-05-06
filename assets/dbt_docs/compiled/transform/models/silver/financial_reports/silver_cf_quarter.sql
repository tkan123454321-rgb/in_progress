





-- STEP 1: DEDUPLICATE BRONZE DATA
-- Retrieve the latest record for each ticker, year, quarter, and indicator_id based
-- on ingestion time.
with
    watermark as (
        select
            COALESCE(
                MAX(silver_updated_at),
                CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
            ) as max_time
        from "lakehouse_main"."silver"."silver_cf_quarter"
    ),

    new_data as (
        select *
        from "lakehouse_main"."bronze"."financial_reports_quarter"
        where
            year >= 2018 and data_type in ('cash_flow_indirect_quarter')
            
                and bronze_ingested_time > (select max_time from watermark)
            
    ),

    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, year, quarter, indicator_id
                order by bronze_ingested_time DESC
            ) as rn
        from new_data
    ),
    -- STEP 2: PIVOT INDICATORS
    -- Transform the data from long format (rows) to wide format (columns).
    pivoted_data as (
        select
            ticker,
            year,
            quarter

            
                ,
                MAX(
                    case
                        when indicator_id = 104
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as cfo
            
                ,
                MAX(
                    case
                        when indicator_id = 212
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as cfi
            
                ,
                MAX(
                    case
                        when indicator_id = 311
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as cff
            
                ,
                MAX(
                    case
                        when indicator_id = 4
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as net_cash_flow
            
                ,
                MAX(
                    case
                        when indicator_id = 10201
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as depreciation
            
                ,
                MAX(
                    case
                        when indicator_id = 201
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as capex
            
                ,
                MAX(
                    case
                        when indicator_id = 101
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as profit_before_tax_cf
            
                ,
                MAX(
                    case
                        when indicator_id = 10301
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as delta_receivables
            
                ,
                MAX(
                    case
                        when indicator_id = 10302
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as delta_inventory
            
                ,
                MAX(
                    case
                        when indicator_id = 10303
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as delta_payables
            
                ,
                MAX(
                    case
                        when indicator_id = 303
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as borrowings_received
            
                ,
                MAX(
                    case
                        when indicator_id = 304
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as debt_repaid
            
                ,
                MAX(
                    case
                        when indicator_id = 308
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as dividends_paid
            
                ,
                MAX(
                    case
                        when indicator_id = 5
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as beginning_cash
            
                ,
                MAX(
                    case
                        when indicator_id = 6
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as exchange_rate_effect
            
                ,
                MAX(
                    case
                        when indicator_id = 7
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as ending_cash
            

        from deduped_data
        where rn = 1
        group by ticker, year, quarter
    ),
    -- STEP 3: APPLY DATA QUALITY RULES
    -- Evaluate data against predefined Data Quality rules to capture the unqualified
    -- reason.
    applied_dq_rules as (
        select
            *, 

    

    
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- PART 1: AUTO-CHECK NULLS FOR MANDATORY COLUMNS
                
                    
                        case
                            when cfo is NULL
                            then 'cfo is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                        case
                            when net_cash_flow is NULL
                            then 'net_cash_flow is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when beginning_cash is NULL
                            then 'beginning_cash is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                        case
                            when ending_cash is NULL
                            then 'ending_cash is null'
                            else NULL
                        end,
                    
                

                -- PART 1.5: AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when beginning_cash < 0
                            then 'beginning_cash cannot be negative'
                            else NULL
                        end,
                    
                
                    
                
                    
                        case
                            when ending_cash < 0
                            then 'ending_cash cannot be negative'
                            else NULL
                        end,
                    
                

                -- PART 2: MATHEMATICAL CHECKS (BUSINESS LOGIC)
                -- Equation 1: ID 4 = ID 104 + ID 212 + ID 311
                case
                    when
                        ABS(
                            COALESCE(net_cash_flow, 0)
                            - (COALESCE(cfo, 0) + COALESCE(cfi, 0) + COALESCE(cff, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_cash_flow, 0))
                    then 'Err: ID 4 (Net CF) != ID 104 + 212 + 311'
                    else NULL
                end,

                -- Equation 2: ID 7 = ID 5 + ID 4 + ID 6
                case
                    when
                        ABS(
                            COALESCE(ending_cash, 0) - (
                                COALESCE(beginning_cash, 0)
                                + COALESCE(net_cash_flow, 0)
                                + COALESCE(exchange_rate_effect, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(ending_cash, 0))
                    then 'Err: ID 7 (Ending Cash) != ID 5 + 4 + 6'
                    else NULL
                end

            ),
            ''
        )

    

 as unqualified_reason
        from pivoted_data
    )

-- STEP 4: FINAL SELECTION & FORMATTING
-- Handle null values, append system audit columns, and determine the final DQ status.
select
    ticker,
    year,
    quarter,

    -- Replace NULL values with 0 for all financial indicators
    
        COALESCE(cfo, 0) as cfo,
    
        COALESCE(cfi, 0) as cfi,
    
        COALESCE(cff, 0) as cff,
    
        COALESCE(net_cash_flow, 0) as net_cash_flow,
    
        COALESCE(depreciation, 0) as depreciation,
    
        COALESCE(capex, 0) as capex,
    
        COALESCE(profit_before_tax_cf, 0) as profit_before_tax_cf,
    
        COALESCE(delta_receivables, 0) as delta_receivables,
    
        COALESCE(delta_inventory, 0) as delta_inventory,
    
        COALESCE(delta_payables, 0) as delta_payables,
    
        COALESCE(borrowings_received, 0) as borrowings_received,
    
        COALESCE(debt_repaid, 0) as debt_repaid,
    
        COALESCE(dividends_paid, 0) as dividends_paid,
    
        COALESCE(beginning_cash, 0) as beginning_cash,
    
        COALESCE(exchange_rate_effect, 0) as exchange_rate_effect,
    
        COALESCE(ending_cash, 0) as ending_cash,
    

     CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,  '4c6d9271-375a-4d96-926e-49714c96b216' as silver_invocation_id, 

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

from applied_dq_rules