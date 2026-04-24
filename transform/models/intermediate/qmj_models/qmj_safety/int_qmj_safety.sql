{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_safety']
) }}
{% set audit_cols = get_audit_columns('intermediate') %}
-- STEP 1: Extract qualified components from prior intermediate models
WITH ttm_metrics AS (
    SELECT * FROM {{ ref('int_ttm_metrics') }} 
    WHERE ttm_status = 'valid_ttm'
),
bab_score AS (
    SELECT 
        *
    FROM {{ ref('int_qmj_beta_final') }}
    WHERE status = 'qualified'
),

z_score AS (
    SELECT * FROM {{ ref('int_z_score') }}
    WHERE status = 'qualified'
),

o_score AS (
    SELECT * FROM {{ ref('int_o_score') }}
    WHERE status = 'qualified'
),

-- STEP 2: Combine all safety components using FULL OUTER JOIN
joined_all AS (
    SELECT 
        ticker,
        year,
        quarter,
        absolute_quarter,

        b.bab_score,
        z.altman_z_score,
        o.ohlson_o_score,

        t.total_assets,
        t.net_income_parent_ttm,
        (t.total_equity - t.minority_interest - t.preferred_stock) AS book_equity,
        t.short_term_debt AS short_term_debt,
        t.long_term_debt AS long_term_debt,
        t.minority_interest AS minority_interest,
        t.preferred_stock AS preferred_stock

    FROM ttm_metrics t
    FULL OUTER JOIN bab_score b USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN z_score z USING (ticker, year, quarter, absolute_quarter)
    FULL OUTER JOIN o_score o USING (ticker, year, quarter, absolute_quarter)
),

-- STEP 3: Calculate Leverage (LEV) and Return on Equity (ROE)
calc_lev_and_roe AS (
    SELECT 
        *,
        -- Leverage Score = -(Total Debt) / Total Assets
        -(short_term_debt + long_term_debt + minority_interest + preferred_stock) / NULLIF(total_assets, 0) AS lev_score,
        
        -- ROE is calculated here as an input for EVOL
        net_income_parent_ttm / NULLIF(book_equity, 0) AS roe
    FROM joined_all
),

-- STEP 4: Calculate Earnings Volatility (EVOL) over the past 16 quarters
calc_evol AS (
    SELECT 
        *,
        -- Raw EVOL: Standard deviation of ROE
        STDDEV_SAMP(roe) OVER w_16q AS evol_raw,
        
        -- Count available quarters to enforce the minimum 12-quarter rule
        COUNT(roe) OVER w_16q AS count_roe_quarters
    FROM calc_lev_and_roe
    WINDOW w_16q AS (
        PARTITION BY ticker 
        ORDER BY absolute_quarter 
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    )
),

-- STEP 5: Apply Data Quality Rules for the combined Safety factor
applied_dq_rules AS (
    SELECT 
        *,
        -- EVOL Score is the negative of the raw volatility
        (-1 * evol_raw) AS evol_score,
        
        -- Embedded DQ checks
        NULLIF(
            CONCAT_WS(' | ',
                -- Check EVOL history
                CASE WHEN count_roe_quarters < 12 THEN 'Err: Not enough ROE history for EVOL (<12 quarters)' ELSE NULL END,

                -- Check 5 core safety components for nulls
                CASE WHEN bab_score IS NULL THEN 'missing_bab_score' ELSE NULL END,
                CASE WHEN altman_z_score IS NULL THEN 'missing_altman_z_score' ELSE NULL END,
                CASE WHEN ohlson_o_score IS NULL THEN 'missing_ohlson_o_score' ELSE NULL END,
                CASE WHEN lev_score IS NULL THEN 'missing_lev_score' ELSE NULL END,
                CASE WHEN evol_raw IS NULL THEN 'missing_evol' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM calc_evol
)
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    
    bab_score,
    lev_score,
    ohlson_o_score,
    altman_z_score,
    evol_score,
    
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,

    unqualified_reason

    -- Audit Columns
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_dq_rules