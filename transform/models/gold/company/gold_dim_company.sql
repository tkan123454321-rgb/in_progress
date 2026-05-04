{{ config(materialized="table", tags=["gold", "dim_company"]) }}

{% set fields1 = get_fundamental_columns("fundamental_1") %}
{% set fields2 = get_fundamental_columns("fundamental_2") %}
{% set audit_cols = get_audit_columns("gold") %}

-- STEP 1: Extract cleaned data from the Silver layer
with
    silver_company_info as (select * from {{ ref("silver_dim_company") }}),

    silver_fundamentals_1 as (select * from {{ ref("silver_fundamental_1") }}),

    silver_fundamentals_2 as (select * from {{ ref("silver_fundamental_2") }}),

    -- STEP 2: Join company dimensions with fundamental metrics
    joined_data as (
        select
            dim.ticker,
            dim.company_name,
            dim.industry_group,
            dim.sector_detail,
            dim.company_type,

            -- Automatically inject all metrics from Fundamental 1
            {% for field in fields1 %} fun1.{{ field.alias }}, {% endfor %}

            -- Automatically inject all metrics from Fundamental 2
            {% for field in fields2 %} fun2.{{ field.alias }}, {% endfor %}

            -- Pull status flags from Silver layers for downstream validation
            fun1.ticker as fun1_ticker,
            fun2.ticker as fun2_ticker,
            fun1.status as fun1_status,
            fun1.unqualified_reason as fun1_unqualified_reason,
            fun2.status as fun2_status,
            fun2.unqualified_reason as fun2_unqualified_reason

        from silver_company_info dim
        left join silver_fundamentals_1 fun1 on dim.ticker = fun1.ticker
        left join silver_fundamentals_2 fun2 on dim.ticker = fun2.ticker
    ),

    -- STEP 3: Apply Gold-layer specific Data Quality Rules and Business Logic
    applied_gold_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- A. Catch missing API data from upstream sources
                    case
                        when fun1_ticker is NULL
                        then '[API Error]: Missing fundamental_1 data'
                        else NULL
                    end,
                    case
                        when fun2_ticker is NULL
                        then '[API Error]: Missing fundamental_2 data'
                        else NULL
                    end,

                    -- B. Carry over unqualified reasons from the Silver layer
                    case
                        when fun1_status = 'unqualified'
                        then '[Silver Fun1 Error]: ' || fun1_unqualified_reason
                        else NULL
                    end,
                    case
                        when fun2_status = 'unqualified'
                        then '[Silver Fun2 Error]: ' || fun2_unqualified_reason
                        else NULL
                    end,

                    -- C. GOLD RULE 1: Include only standard corporations ('CT'),
                    -- excluding Banks/Securities/Insurance
                    case
                        when company_type != 'CT'
                        then
                            '[Gold Rule]: Excluded Financials/Funds (Not a standard Corporate)'
                        else NULL
                    end,

                    -- D. GOLD RULE 2: Minimum Size and Liquidity Thresholds
                    case
                        when market_cap < 500000000000
                        then '[Gold Rule]: market_cap < 500B VND (Penny/Illiquid)'
                        else NULL
                    end,
                    case
                        when avg_volume_3m < 10000
                        then '[Gold Rule]: avg_volume_3m < 10k (Dead Stock)'
                        else NULL
                    end
                ),
                ''
            ) as gold_unqualified_reason
        from joined_data
    )

-- STEP 4: Final Selection and Status Resolution
select
    ticker,
    company_name,
    industry_group,
    sector_detail,
    company_type,

    -- Output Fundamental 1 metrics
    {% for field in fields1 %} {{ field.alias }}, {% endfor %}

    -- Output Fundamental 2 metrics
    {% for field in fields2 %} {{ field.alias }}, {% endfor %}

    -- Resolve Final Status
    case
        when gold_unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    gold_unqualified_reason

    -- Auto-generated audit columns for the Gold layer
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_gold_rules
