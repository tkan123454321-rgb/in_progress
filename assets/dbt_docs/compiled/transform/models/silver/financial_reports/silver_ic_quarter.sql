





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
        from "lakehouse_main"."silver"."silver_ic_quarter"
    ),
    new_data as (
        select *
        from "lakehouse_main"."bronze"."financial_reports_quarter"
        where
            data_type = 'income_statement_quarter' and year >= 2018
            
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
                        when indicator_id = 1
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as gross_revenue
            
                ,
                MAX(
                    case
                        when indicator_id = 2
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as revenue_deduction
            
                ,
                MAX(
                    case
                        when indicator_id = 3
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as net_revenue
            
                ,
                MAX(
                    case
                        when indicator_id = 4
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as cogs
            
                ,
                MAX(
                    case
                        when indicator_id = 5
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as gross_profit
            
                ,
                MAX(
                    case
                        when indicator_id = 6
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as financial_income
            
                ,
                MAX(
                    case
                        when indicator_id = 7
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as financial_expense
            
                ,
                MAX(
                    case
                        when indicator_id = 701
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as interest_expense
            
                ,
                MAX(
                    case
                        when indicator_id = 8
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as affiliate_profit
            
                ,
                MAX(
                    case
                        when indicator_id = 9
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as selling_expense
            
                ,
                MAX(
                    case
                        when indicator_id = 10
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as admin_expense
            
                ,
                MAX(
                    case
                        when indicator_id = 11
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as operating_profit
            
                ,
                MAX(
                    case
                        when indicator_id = 12
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as other_income
            
                ,
                MAX(
                    case
                        when indicator_id = 13
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as other_expense
            
                ,
                MAX(
                    case
                        when indicator_id = 14
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as other_profit
            
                ,
                MAX(
                    case
                        when indicator_id = 15
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as profit_before_tax
            
                ,
                MAX(
                    case
                        when indicator_id = 16
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as current_tax
            
                ,
                MAX(
                    case
                        when indicator_id = 17
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as deferred_tax
            
                ,
                MAX(
                    case
                        when indicator_id = 18
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as income_tax_expense
            
                ,
                MAX(
                    case
                        when indicator_id = 19
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as net_income
            
                ,
                MAX(
                    case
                        when indicator_id = 20
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as minority_interest
            
                ,
                MAX(
                    case
                        when indicator_id = 21
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as net_income_parent
            

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
                            when net_revenue is NULL
                            then 'net_revenue is null'
                            else NULL
                        end,
                    
                
                    
                        case
                            when cogs is NULL
                            then 'cogs is null'
                            else NULL
                        end,
                    
                
                    
                        case
                            when gross_profit is NULL
                            then 'gross_profit is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when operating_profit is NULL
                            then 'operating_profit is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when profit_before_tax is NULL
                            then 'profit_before_tax is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when net_income is NULL
                            then 'net_income is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                        case
                            when net_income_parent is NULL
                            then 'net_income_parent is null'
                            else NULL
                        end,
                    
                

                -- PART 1.5: AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
                
                    
                        case
                            when gross_revenue < 0
                            then 'gross_revenue cannot be negative'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                

                -- PART 2: MATHEMATICAL CHECKS (BUSINESS LOGIC)
                -- 3 = (1) - (2): Check Net Revenue
                case
                    when
                        ABS(
                            COALESCE(net_revenue, 0) - (
                                COALESCE(gross_revenue, 0)
                                - COALESCE(revenue_deduction, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 3 (Net Rev) != (1) - (2)'
                    else NULL
                end,

                -- 5 = (3) - (4): Check Gross Profit
                case
                    when
                        ABS(
                            COALESCE(gross_profit, 0)
                            - (COALESCE(net_revenue, 0) - COALESCE(cogs, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 5 (Gross Profit) != (3) - (4)'
                    else NULL
                end,

                -- 11 = (5) + (6) - (7) + (8) - (9) - (10): Check Net Operating Profit
                case
                    when
                        ABS(
                            COALESCE(operating_profit, 0) - (
                                COALESCE(gross_profit, 0)
                                + COALESCE(financial_income, 0)
                                - COALESCE(financial_expense, 0)
                                + COALESCE(affiliate_profit, 0)
                                - COALESCE(selling_expense, 0)
                                - COALESCE(admin_expense, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 11 (Operating Profit) != (5)+(6)-(7)+(8)-(9)-(10)'
                    else NULL
                end,

                -- 14 = (12) - (13): Check Other Profit
                case
                    when
                        ABS(
                            COALESCE(other_profit, 0)
                            - (COALESCE(other_income, 0) - COALESCE(other_expense, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 14 (Other Profit) != (12) - (13)'
                    else NULL
                end,

                -- 15 = (11) + (14): Check Total Pre-tax Profit
                case
                    when
                        ABS(
                            COALESCE(profit_before_tax, 0) - (
                                COALESCE(operating_profit, 0)
                                + COALESCE(other_profit, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 15 (Pre-tax Profit) != (11) + (14)'
                    else NULL
                end,

                -- 18 = (16) + (17): Check Total Tax Expense
                case
                    when
                        ABS(
                            COALESCE(income_tax_expense, 0)
                            - (COALESCE(current_tax, 0) + COALESCE(deferred_tax, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 18 (Tax Expense) != (16) + (17)'
                    else NULL
                end,

                -- 19 = (15) - (18): Check Net Income (Corporate)
                case
                    when
                        ABS(
                            COALESCE(net_income, 0) - (
                                COALESCE(profit_before_tax, 0)
                                - COALESCE(income_tax_expense, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 19 (Net Income) != (15) - (18)'
                    else NULL
                end,

                -- 21 = (19) - (20): Check Net Income to Parent Company Shareholders
                case
                    when
                        ABS(
                            COALESCE(net_income_parent, 0)
                            - (COALESCE(net_income, 0) - COALESCE(minority_interest, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 21 (Net Income Parent) != (19) - (20)'
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
    
        COALESCE(gross_revenue, 0) as gross_revenue,
    
        COALESCE(revenue_deduction, 0) as revenue_deduction,
    
        COALESCE(net_revenue, 0) as net_revenue,
    
        COALESCE(cogs, 0) as cogs,
    
        COALESCE(gross_profit, 0) as gross_profit,
    
        COALESCE(financial_income, 0) as financial_income,
    
        COALESCE(financial_expense, 0) as financial_expense,
    
        COALESCE(interest_expense, 0) as interest_expense,
    
        COALESCE(affiliate_profit, 0) as affiliate_profit,
    
        COALESCE(selling_expense, 0) as selling_expense,
    
        COALESCE(admin_expense, 0) as admin_expense,
    
        COALESCE(operating_profit, 0) as operating_profit,
    
        COALESCE(other_income, 0) as other_income,
    
        COALESCE(other_expense, 0) as other_expense,
    
        COALESCE(other_profit, 0) as other_profit,
    
        COALESCE(profit_before_tax, 0) as profit_before_tax,
    
        COALESCE(current_tax, 0) as current_tax,
    
        COALESCE(deferred_tax, 0) as deferred_tax,
    
        COALESCE(income_tax_expense, 0) as income_tax_expense,
    
        COALESCE(net_income, 0) as net_income,
    
        COALESCE(minority_interest, 0) as minority_interest,
    
        COALESCE(net_income_parent, 0) as net_income_parent,
    

    -- Cột Audit
     CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,  '4c6d9271-375a-4d96-926e-49714c96b216' as silver_invocation_id, 

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason
from applied_dq_rules