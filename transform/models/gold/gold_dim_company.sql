{{ config(materialized='table') }}

WITH dim AS (
    SELECT * FROM {{ ref('silver_dim_company') }}
),

fun AS (
    SELECT * FROM {{ ref('silver_fundamental') }}
),

joined_data as (
    SELECT 
        dim.ticker,
        dim.company_name,
        dim.industry_group,
        dim.sector_detail,

        -- 2. Chỉ số tài chính từ bảng Fundamental
        fun.ticker as fun_ticker,
        fun.shares_outstanding,
        fun.floating_shares,
        fun.market_cap,
        fun.avg_volume_3m,
        fun.insider_ownership,
        fun.institution_ownership,
        fun.foreign_ownership,

        -- 3. Kéo theo cái nhãn kiểm định của lớp Silver để tái sử dụng
        fun.status AS fun_status,
        fun.unqualified_reason AS fun_unqualified_reason
    
    FROM dim
    LEFT JOIN fun ON dim.ticker = fun.ticker

),

applied_gold_rules as (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS('; ',
                CASE WHEN fun_ticker IS NULL THEN '[API Error]: Missing fundamental data from source' END,
                CASE WHEN fun_status = 'unqualified' THEN '[Silver Error]: ' || fun_unqualified_reason END,
                CASE WHEN market_cap < 500000000000 THEN '[Gold Rule]: market_cap < 500B VND (Penny/Trash)' END,
                CASE WHEN avg_volume_3m < 100000 THEN '[Gold Rule]: avg_volume_3m < 100k (Illiquid)' END
            ), 
            ''
        ) AS gold_unqualified_reason
    FROM joined_data
)

SELECT 
    ticker,
    company_name,
    industry_group,
    sector_detail,
    shares_outstanding,
    floating_shares,
    market_cap,
    avg_volume_3m,
    insider_ownership,
    institution_ownership,
    foreign_ownership,

    CASE 
        WHEN gold_unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    gold_unqualified_reason,
    {{ generate_audit_columns('gold') }}
FROM applied_gold_rules
