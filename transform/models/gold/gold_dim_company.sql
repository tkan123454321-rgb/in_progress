{{ config(materialized='table') }}
{% set fields1 = get_fundamental_columns('fundamental_1') %}
{% set fields2 = get_fundamental_columns('fundamental_2') %}
{% set audit_cols = get_audit_columns('gold') %}

-- 2. Bốc 3 bảng Silver đã được dọn sạch lên
WITH dim AS (
    SELECT * FROM {{ ref('silver_dim_company') }}
),

fun1 AS (
    SELECT * FROM {{ ref('silver_fundamental_1') }}
),

fun2 AS (
    SELECT * FROM {{ ref('silver_fundamental_2') }}
),
joined_data AS (
    SELECT 
        dim.ticker,
        dim.company_name,
        dim.industry_group,
        dim.sector_detail,
        dim.company_type, 

        -- Tự động kéo toàn bộ các chỉ số từ bảng Fundamental 1
        {% for field in fields1 %}
        fun1.{{ field.alias }},
        {% endfor %}

        -- Tự động kéo toàn bộ các chỉ số từ bảng Fundamental 2
        {% for field in fields2 %}
        fun2.{{ field.alias }},
        {% endfor %}

        -- Kéo theo nhãn kiểm định của 2 bảng Fundamental để check lỗi
        fun1.ticker AS fun1_ticker,
        fun2.ticker AS fun2_ticker,
        fun1.status AS fun1_status,
        fun1.unqualified_reason AS fun1_unqualified_reason,
        fun2.status AS fun2_status,
        fun2.unqualified_reason AS fun2_unqualified_reason
    
    FROM dim
    LEFT JOIN fun1 ON dim.ticker = fun1.ticker
    LEFT JOIN fun2 ON dim.ticker = fun2.ticker
),
applied_gold_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS('; ',
                -- A. Bắt lỗi mất tích data từ API
                CASE WHEN fun1_ticker IS NULL THEN '[API Error]: Missing fundamental_1 data' END,
                CASE WHEN fun2_ticker IS NULL THEN '[API Error]: Missing fundamental_2 data' END,

                -- B. Chở rác từ tầng Silver lên báo cáo
                CASE WHEN fun1_status = 'unqualified' THEN '[Silver Fun1 Error]: ' || fun1_unqualified_reason END,
                CASE WHEN fun2_status = 'unqualified' THEN '[Silver Fun2 Error]: ' || fun2_unqualified_reason END,

                -- C. LUẬT MỚI: Chỉ chơi với Công ty thông thường (CT), loại bỏ Bank/Securities/Insurance
                CASE WHEN company_type != 'CT' THEN '[Gold Rule]: Excluded Financials/Funds (Not a standard Corporate)' END,

                -- D. LUẬT MỚI: Nới lỏng thanh khoản xuống 10k để tìm ngọc trong đá
                CASE WHEN market_cap < 500000000000 THEN '[Gold Rule]: market_cap < 500B VND (Penny/Trash)' END,
                CASE WHEN avg_volume_3m < 10000 THEN '[Gold Rule]: avg_volume_3m < 10k (Dead Stock)' END
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
    company_type,
    
    {% for field in fields1 %}
    {{ field.alias }},
    {% endfor %}

    {% for field in fields2 %}
    {{ field.alias }},
    {% endfor %}

    CASE 
        WHEN gold_unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    gold_unqualified_reason,
    
    -- Vòng lặp đẻ cột Audit cho lớp Gold
    {% for col in audit_cols %}
    {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

FROM applied_gold_rules