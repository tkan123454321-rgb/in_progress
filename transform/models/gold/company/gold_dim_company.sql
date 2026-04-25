{{ config(
    materialized='table',
    tags=['gold', 'dim_company']
) }}

{% set fields1 = get_fundamental_columns('fundamental_1') %}
{% set fields2 = get_fundamental_columns('fundamental_2') %}
{% set audit_cols = get_audit_columns('gold') %}

-- STEP 1: Extract cleaned data from the Silver layer
WITH silver_company_info AS (
    SELECT * FROM {{ ref('silver_dim_company') }}
),

silver_fundamentals_1 AS (
    SELECT * FROM {{ ref('silver_fundamental_1') }}
),

silver_fundamentals_2 AS (
    SELECT * FROM {{ ref('silver_fundamental_2') }}
),

-- STEP 2: Join company dimensions with fundamental metrics
joined_data AS (
    SELECT 
        dim.ticker,
        dim.company_name,
        dim.industry_group,
        dim.sector_detail,
        dim.company_type, 

        -- Automatically inject all metrics from Fundamental 1
        {% for field in fields1 %}
        fun1.{{ field.alias }},
        {% endfor %}

        -- Automatically inject all metrics from Fundamental 2
        {% for field in fields2 %}
        fun2.{{ field.alias }},
        {% endfor %}

        -- Pull status flags from Silver layers for downstream validation
        fun1.ticker AS fun1_ticker,
        fun2.ticker AS fun2_ticker,
        fun1.status AS fun1_status,
        fun1.unqualified_reason AS fun1_unqualified_reason,
        fun2.status AS fun2_status,
        fun2.unqualified_reason AS fun2_unqualified_reason
    
    FROM silver_company_info dim
    LEFT JOIN silver_fundamentals_1 fun1 
        ON dim.ticker = fun1.ticker
    LEFT JOIN silver_fundamentals_2 fun2 
        ON dim.ticker = fun2.ticker
),

-- STEP 3: Apply Gold-layer specific Data Quality Rules and Business Logic
applied_gold_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- A. Catch missing API data from upstream sources
                CASE WHEN fun1_ticker IS NULL THEN '[API Error]: Missing fundamental_1 data' ELSE NULL END,
                CASE WHEN fun2_ticker IS NULL THEN '[API Error]: Missing fundamental_2 data' ELSE NULL END,

                -- B. Carry over unqualified reasons from the Silver layer
                CASE WHEN fun1_status = 'unqualified' THEN '[Silver Fun1 Error]: ' || fun1_unqualified_reason ELSE NULL END,
                CASE WHEN fun2_status = 'unqualified' THEN '[Silver Fun2 Error]: ' || fun2_unqualified_reason ELSE NULL END,

                -- C. GOLD RULE 1: Include only standard corporations ('CT'), excluding Banks/Securities/Insurance
                CASE WHEN company_type != 'CT' THEN '[Gold Rule]: Excluded Financials/Funds (Not a standard Corporate)' ELSE NULL END,

                -- D. GOLD RULE 2: Minimum Size and Liquidity Thresholds
                CASE WHEN market_cap < 500000000000 THEN '[Gold Rule]: market_cap < 500B VND (Penny/Illiquid)' ELSE NULL END,
                CASE WHEN avg_volume_3m < 10000 THEN '[Gold Rule]: avg_volume_3m < 10k (Dead Stock)' ELSE NULL END
            ), 
            ''
        ) AS gold_unqualified_reason
    FROM joined_data
)

-- STEP 4: Final Selection and Status Resolution
SELECT 
    ticker,
    company_name,
    industry_group,
    sector_detail,
    company_type,
    
    -- Output Fundamental 1 metrics
    {% for field in fields1 %}
    {{ field.alias }},
    {% endfor %}

    -- Output Fundamental 2 metrics
    {% for field in fields2 %}
    {{ field.alias }},
    {% endfor %}

    -- Resolve Final Status
    CASE 
        WHEN gold_unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    gold_unqualified_reason
    
    -- Auto-generated audit columns for the Gold layer
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_gold_rules