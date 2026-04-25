{{ config(
    materialized='table',
    tags=['gold', 'web_api']
) }}

{% set audit_cols = get_audit_columns('gold') %}

-- STEP 1: Extract core QMJ factors and pre-calculate ranking 
WITH gold_qmj AS (
    SELECT 
        ticker, 
        year, 
        quarter, 
        absolute_quarter,
        qmj_profitability_score, 
        qmj_growth_score, 
        qmj_safety_score, 
        qmj_score,
        -- Calculate QMJ Rank per quarter for easy Top 10/30/50 filtering on UI
        RANK() OVER (PARTITION BY absolute_quarter ORDER BY qmj_score DESC) AS qmj_rank
    FROM {{ ref('gold_qmj_z_final') }} 
    WHERE status = 'qualified'
),

-- STEP 2: Extract historical Value and Momentum factors
gold_val_mom AS (
    SELECT 
        ticker, 
        year, 
        quarter, 
        absolute_quarter,
        value_raw_score, 
        momentum_raw_score,
        z_value, 
        z_momentum
    FROM {{ ref('gold_value_and_momentum_z') }} 
    WHERE status = 'qualified'
),

-- STEP 3: Extract recent/live Value and Momentum factors for the latest snapshot
gold_val_mom_recent AS (
    SELECT 
        ticker,
        value_recent_score, 
        momentum_recent,
        z_value_recent, 
        z_momentum_recent
    FROM {{ ref('gold_value_and_momentum_z_recent') }}
    WHERE status = 'qualified'
),

-- STEP 4: Extract current company dimensions and liquidity info
gold_company AS (
    SELECT 
        ticker, 
        company_name, 
        industry_group, 
        sector_detail, 
        exchange,
        market_cap AS current_market_cap,
        avg_volume_3m,
        shares_outstanding AS current_shares_outstanding
    FROM {{ ref('gold_dim_company') }} 
    WHERE status = 'qualified'
),

-- STEP 5: Extract quarterly historical market info
silver_quarter AS (
    SELECT 
        ticker, 
        year, 
        quarter,
        market_cap AS quarter_market_cap,
        shares_outstanding AS quarter_shares_outstanding
    FROM {{ ref('silver_fundamental_quarter') }}
    WHERE status = 'qualified'
),

-- STEP 6: Assemble the One Big Table (OBT) for the Web API
super_obt AS (
    SELECT 
        -- A. Company Identification
        q.ticker,
        c.company_name,
        c.exchange,
        c.sector_detail,
        c.industry_group,

        -- B. Time Axis
        q.year,
        q.quarter,
        q.absolute_quarter,

        -- C. Liquidity & Size (Current & Quarterly)
        c.current_market_cap,
        c.avg_volume_3m,
        c.current_shares_outstanding,
        qi.quarter_market_cap,
        qi.quarter_shares_outstanding,

        -- D. Quality (QMJ) Pillar
        ROUND(q.qmj_profitability_score, 3) AS qmj_profitability,
        ROUND(q.qmj_growth_score, 3) AS qmj_growth,
        ROUND(q.qmj_safety_score, 3) AS qmj_safety,
        ROUND(q.qmj_score, 3) AS qmj_score,
        q.qmj_rank, 
        
        -- E. Value & Momentum Pillar (Historical vs Recent)
        ROUND(vm.z_value, 3) AS z_value_historical,
        ROUND(vm.z_momentum, 3) AS z_momentum_historical,
        ROUND(vm.value_raw_score, 3) AS value_raw_score,
        ROUND(vm.momentum_raw_score, 3) AS momentum_raw_score,
        
        ROUND(rvm.value_recent_score, 3) AS value_recent_score,
        ROUND(rvm.momentum_recent, 3) AS momentum_recent_score,
        ROUND(rvm.z_value_recent, 3) AS z_value_recent,
        ROUND(rvm.z_momentum_recent, 3) AS z_momentum_recent

    FROM gold_qmj q
    LEFT JOIN gold_val_mom vm USING (ticker, year, quarter, absolute_quarter)
    LEFT JOIN gold_company c USING (ticker)
    LEFT JOIN silver_quarter qi USING (ticker, year, quarter)
    LEFT JOIN gold_val_mom_recent rvm USING (ticker)
)

-- STEP 7: Final Output with Audit Columns
SELECT 
    *
    -- Auto-generated audit columns
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}
FROM super_obt
ORDER BY absolute_quarter DESC, qmj_rank ASC