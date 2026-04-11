{{ config(
    materialized='table',
    tags=['obt', 'web_api']
) }}

WITH qmj_data AS (
    SELECT 
        ticker, year, quarter, absolute_quarter,
        qmj_profitability_score, qmj_growth_score, qmj_safety_score, qmj_score,
        -- Đóng đinh cái Rank này để Web làm bộ lọc Top 10, 30, 50
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY qmj_score DESC) AS qmj_rank
    FROM {{ ref('gold_qmj_z_final') }} 
    WHERE status = 'qualified'
),

val_mom_data AS (
    SELECT 
        ticker, year, quarter, absolute_quarter,
        value_raw_score, momentum_raw_score,
        z_value, z_momentum
    FROM {{ ref('gold_value_and_momentum_z') }} 
    WHERE status = 'qualified'
),
recent_val_mom_data AS (
    -- DỮ LIỆU ĐỊNH GIÁ & ĐỘNG LƯỢNG (HIỆN TẠI - DAILY RECENT)
    SELECT 
        ticker,
        value_recent_score, 
        momentum_recent,
        z_value_recent, 
        z_momentum_recent
    FROM {{ ref('gold_value_and_momentum_z_recent') }}
    WHERE status = 'qualified'
),

company_info AS (
    SELECT 
        ticker, company_name, industry_group, sector_detail, exchange,
        market_cap AS current_market_cap,
        avg_volume_3m,
        shares_outstanding AS current_shares_outstanding,
        floating_shares, insider_ownership, institution_ownership, foreign_ownership
    FROM {{ ref('gold_dim_company') }} 
    WHERE status = 'qualified'
),

quarter_info AS (
    SELECT 
        ticker, year, quarter,
        market_cap AS quarter_market_cap,
        shares_outstanding AS quarter_shares_outstanding,
        preferred_stock -- Đã bổ sung cột này để không bị lỗi ở dưới
    FROM {{ ref('silver_fundamental_quarter') }}
    WHERE status = 'qualified'
),

-- ==============================================================================
-- 2. HỘI QUÂN VÀO SIÊU THỰC THỂ (SUPER OBT)
-- ==============================================================================
super_obt AS (
    SELECT 
        -- A. ĐỊNH DANH DOANH NGHIỆP (Đã bỏ c. ở cột ticker)
        ticker,
        c.company_name,
        c.exchange,
        c.sector_detail,
        c.industry_group,

        -- B. TRỤC THỜI GIAN
        year,
        quarter,
        absolute_quarter,

        -- C. THANH KHOẢN & SỞ HỮU (Hiện tại)
        c.current_market_cap,
        c.avg_volume_3m,
        c.current_shares_outstanding,
        qi.quarter_market_cap,
        qi.quarter_shares_outstanding,

        -- E. SỨC MẠNH CHẤT LƯỢNG (QMJ)

        ROUND(q.qmj_profitability_score, 3) AS qmj_profitability,
        ROUND(q.qmj_growth_score, 3) AS qmj_growth,
        ROUND(q.qmj_safety_score, 3) AS qmj_safety,
        ROUND(q.qmj_score, 3) AS qmj_score,
        q.qmj_rank, 
        
        -- F. SỨC MẠNH ĐỊNH GIÁ & ĐÀ TĂNG TRƯỞNG
        ROUND(vm.z_value, 3) AS z_value_historical,
        ROUND(vm.z_momentum, 3) AS z_momentum_historical,
        ROUND(vm.value_raw_score, 3) AS value_raw_score,
        ROUND(vm.momentum_raw_score, 3) AS momentum_raw_score,
        ROUND(rvm.value_recent_score, 3) AS value_recent_score,
        ROUND(rvm.momentum_recent, 3) AS momentum_recent_score,
        ROUND(rvm.z_value_recent, 3) AS z_value_recent,
        ROUND(rvm.z_momentum_recent, 3) AS z_momentum_recent

    FROM qmj_data q
    LEFT JOIN val_mom_data vm USING (ticker, year, quarter, absolute_quarter)
    LEFT JOIN company_info c USING (ticker)
    LEFT JOIN quarter_info qi USING (ticker, year, quarter)
    LEFT JOIN recent_val_mom_data rvm USING (ticker)
)

SELECT 
    * {% set audit_cols = get_audit_columns('obt') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
FROM super_obt
ORDER BY absolute_quarter DESC, qmj_rank ASC