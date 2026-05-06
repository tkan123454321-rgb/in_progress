




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
        from "lakehouse_main"."silver"."silver_bl_quarter"
    ),

    new_data as (
        select *
        from "lakehouse_main"."bronze"."financial_reports_quarter"
        where
            data_type = 'balance_sheet_quarter' and year >= 2018
            
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
                        when indicator_id = 2
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as total_assets
            
                ,
                MAX(
                    case
                        when indicator_id = 4
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as total_capital
            
                ,
                MAX(
                    case
                        when indicator_id = 101
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as current_assets
            
                ,
                MAX(
                    case
                        when indicator_id = 102
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as long_term_assets
            
                ,
                MAX(
                    case
                        when indicator_id = 301
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as total_liabilities
            
                ,
                MAX(
                    case
                        when indicator_id = 302
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as total_equity
            
                ,
                MAX(
                    case
                        when indicator_id = 30101
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as current_liabilities
            
                ,
                MAX(
                    case
                        when indicator_id = 30102
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as long_term_liabilities
            
                ,
                MAX(
                    case
                        when indicator_id = 10101
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as cash_and_equivalents
            
                ,
                MAX(
                    case
                        when indicator_id = 3010105
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as income_taxes_payable
            
                ,
                MAX(
                    case
                        when indicator_id = 3020111
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as retained_earnings
            
                ,
                MAX(
                    case
                        when indicator_id = 3020114
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as minority_interest
            
                ,
                MAX(
                    case
                        when indicator_id = 10202
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as fixed_assets
            
                ,
                MAX(
                    case
                        when indicator_id = 10103
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as short_term_receivables
            
                ,
                MAX(
                    case
                        when indicator_id = 3010101
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as short_term_debt
            
                ,
                MAX(
                    case
                        when indicator_id = 3010102
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as current_portion_lt_debt
            
                ,
                MAX(
                    case
                        when indicator_id = 3010115
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as repo_transactions
            
                ,
                MAX(
                    case
                        when indicator_id = 3010206
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as long_term_debt
            
                ,
                MAX(
                    case
                        when indicator_id = 3010207
                        then CAST(value as DECIMAL(20,4))
                    end
                ) as convertible_bonds
            

        from deduped_data
        where rn = 1
        group by ticker, year, quarter
    ),
    -- STEP 3: APPLY DATA QUALITY RULES
    -- Evaluate data against predefined Data Quality rules to capture the unqualified
    -- reason.
    applied_dq_rules as (
        select *, 

    

    
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- PART 1: AUTO-CHECK NULLS FOR MANDATORY COLUMNS
                
                    
                        case
                            when total_assets is NULL
                            then 'total_assets is null'
                            else NULL
                        end,
                    
                
                    
                        case
                            when total_capital is NULL
                            then 'total_capital is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                        case
                            when total_liabilities is NULL
                            then 'total_liabilities is null'
                            else NULL
                        end,
                    
                
                    
                        case
                            when total_equity is NULL
                            then 'total_equity is null'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                
                    
                

                -- PART 1.5: AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
                
                    
                        case
                            when total_assets < 0
                            then 'total_assets cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when total_capital < 0
                            then 'total_capital cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when current_assets < 0
                            then 'current_assets cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when long_term_assets < 0
                            then 'long_term_assets cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when total_liabilities < 0
                            then 'total_liabilities cannot be negative'
                            else NULL
                        end,
                    
                
                    
                
                    
                        case
                            when current_liabilities < 0
                            then 'current_liabilities cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when long_term_liabilities < 0
                            then 'long_term_liabilities cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when cash_and_equivalents < 0
                            then 'cash_and_equivalents cannot be negative'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                
                    
                        case
                            when fixed_assets < 0
                            then 'fixed_assets cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when short_term_receivables < 0
                            then 'short_term_receivables cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when short_term_debt < 0
                            then 'short_term_debt cannot be negative'
                            else NULL
                        end,
                    
                
                    
                        case
                            when current_portion_lt_debt < 0
                            then 'current_portion_lt_debt cannot be negative'
                            else NULL
                        end,
                    
                
                    
                
                    
                
                    
                

                -- PART 2: MATHEMATICAL CHECKS (BUSINESS LOGIC)
                -- Check 1: Global Balance Sheet Equation
                case
                    when
                        ABS(COALESCE(total_assets, 0) - COALESCE(total_capital, 0))
                        > 0.01 * ABS(COALESCE(total_assets, 0))
                    then 'Err: Lệch Cân đối (Tài sản != Nguồn vốn)'
                    else NULL
                end,

                -- Check 2: Asset Structure
                case
                    when
                        ABS(
                            COALESCE(total_assets, 0) - (
                                COALESCE(current_assets, 0)
                                + COALESCE(long_term_assets, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(total_assets, 0))
                    then 'Err: Tổng Tài sản != Ngắn hạn + Dài hạn'
                    else NULL
                end,

                -- Check 3: Capital/Equity Structure
                case
                    when
                        ABS(
                            COALESCE(total_capital, 0) - (
                                COALESCE(total_liabilities, 0)
                                + COALESCE(total_equity, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(total_capital, 0))
                    then 'Err: Tổng Nguồn vốn != Nợ phải trả + Vốn CSH'
                    else NULL
                end,

                -- Check 4: Liabilities Structure
                case
                    when
                        ABS(
                            COALESCE(total_liabilities, 0) - (
                                COALESCE(current_liabilities, 0)
                                + COALESCE(long_term_liabilities, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(total_liabilities, 0))
                    then 'Err: Nợ phải trả != Nợ ngắn hạn + Nợ dài hạn'
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

    
        COALESCE(total_assets, 0) as total_assets,
    
        COALESCE(total_capital, 0) as total_capital,
    
        COALESCE(current_assets, 0) as current_assets,
    
        COALESCE(long_term_assets, 0) as long_term_assets,
    
        COALESCE(total_liabilities, 0) as total_liabilities,
    
        COALESCE(total_equity, 0) as total_equity,
    
        COALESCE(current_liabilities, 0) as current_liabilities,
    
        COALESCE(long_term_liabilities, 0) as long_term_liabilities,
    
        COALESCE(cash_and_equivalents, 0) as cash_and_equivalents,
    
        COALESCE(income_taxes_payable, 0) as income_taxes_payable,
    
        COALESCE(retained_earnings, 0) as retained_earnings,
    
        COALESCE(minority_interest, 0) as minority_interest,
    
        COALESCE(fixed_assets, 0) as fixed_assets,
    
        COALESCE(short_term_receivables, 0) as short_term_receivables,
    
        COALESCE(short_term_debt, 0) as short_term_debt,
    
        COALESCE(current_portion_lt_debt, 0) as current_portion_lt_debt,
    
        COALESCE(repo_transactions, 0) as repo_transactions,
    
        COALESCE(long_term_debt, 0) as long_term_debt,
    
        COALESCE(convertible_bonds, 0) as convertible_bonds,
    

     CAST(from_iso8601_timestamp('2026-05-06T08:55:22.931753+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,  '273468de-8a49-4a91-9bc2-2aabb801915e' as silver_invocation_id, 

    case
        when unqualified_reason is not NULL then 'unqualified' else 'qualified'
    end as status,
    unqualified_reason
from applied_dq_rules