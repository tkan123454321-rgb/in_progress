{{ config(
    materialized='table',
    tags=['intermediate', 'daily_value_scoring']
) }}

WITH company_info AS (
    -- 1. LẤY MARKET CAP TỪ GOLD
    SELECT 
        ticker,
        market_cap,
        gold_updated_at,
        DATE_DIFF('day', CAST(gold_updated_at AS DATE), CURRENT_DATE) AS days_since_update
    FROM {{ ref('gold_dim_company') }}
    WHERE ticker != 'VNINDEX'
),

latest_fundamentals AS (
    -- 2. LẤY BCTC QUÝ GẦN NHẤT & TÍNH ĐỘ TRỄ QUÝ
    SELECT 
        ticker,
        year AS report_year,
        quarter AS report_quarter,
        absolute_quarter AS latest_absolute_quarter,
        
        -- Tính Vốn chủ sở hữu cốt lõi (Book Equity)
        (total_equity - minority_interest - preferred_stock) AS latest_book_equity,
        
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY absolute_quarter DESC) as rn_q
        
    FROM {{ ref('int_ttm_metrics') }} 
    WHERE ttm_status = 'valid_ttm'
),

calc_live_value AS (
    -- 3. HỘI QUÂN VÀ TÍNH TOÁN ĐỘ TRỄ BÁO CÁO
    SELECT 
        c.ticker,
        c.gold_updated_at AS last_market_cap_update,
        c.days_since_update,
        c.market_cap,
        
        f.report_year,
        f.report_quarter,
        f.latest_absolute_quarter,
        f.latest_book_equity,

        -- Tự động tính Quý tuyệt đối của ngày hôm nay (Ví dụ năm 2026, Quý 2 = 2026*4 + 2 = 8106)
        (EXTRACT(YEAR FROM CURRENT_DATE) * 4 + EXTRACT(QUARTER FROM CURRENT_DATE)) AS current_absolute_quarter,
        ((EXTRACT(YEAR FROM CURRENT_DATE) * 4 + EXTRACT(QUARTER FROM CURRENT_DATE)) - f.latest_absolute_quarter) AS quarters_delayed,

        -- Value Score
        (f.latest_book_equity / NULLIF(c.market_cap, 0)) AS value_recent_score

    FROM company_info c
    INNER JOIN latest_fundamentals f ON c.ticker = f.ticker AND f.rn_q = 1
)

SELECT 
    ticker,
    last_market_cap_update,
    days_since_update,
    report_year,
    report_quarter,
    latest_absolute_quarter,
    current_absolute_quarter,
    quarters_delayed,
    market_cap,
    latest_book_equity,
    value_recent_score,

    -- 4. GỌI MACRO ĐỂ BẮT LỖI TỔNG HỢP
    {{ check_value_and_momentum_column('value_recent') }} AS unqualified_reason,

    -- 5. CHỐT STATUS
    CASE 
        WHEN {{ check_value_and_momentum_column('value_recent') }} IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status

    -- Audit columns
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM calc_live_value