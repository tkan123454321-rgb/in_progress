{{ config(
    materialized='table',
    tags=['intermediate', 'qmj_beta_final'],
    order_by=['ticker', 'date'] 
) }}

{% set audit_cols = get_audit_columns('intermediate') %}

-- STEP 1: Extract qualified volatility data and remove ephemeral DQ columns
WITH daily_data_raw AS (
    SELECT * FROM {{ ref('int_qmj_beta_vol') }}
    WHERE status = 'qualified'
),

daily_data_cleaned AS (
    -- Exclude inherited status and reason columns to avoid conflicts
    SELECT * FROM TABLE(
        exclude_columns(
            input => TABLE(daily_data_raw),
            columns => DESCRIPTOR(status, unqualified_reason)
        )
    )
),

-- STEP 2: Calculate 3-day overlapping returns
three_day_calculation AS (
    SELECT 
        *,
        SUM(stock_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_ret_3d,
        SUM(mkt_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mkt_ret_3d
    FROM daily_data_cleaned
),

-- STEP 3: Calculate 4-year rolling correlation (1008 trading days)
correlation_calculation AS (
    SELECT 
        *,
        -- Calculate Pearson correlation coefficient over 4 years
        CORR(stock_ret_3d, mkt_ret_3d) OVER w_4y AS rho_4y,
        
        -- Count actual data points to enforce the 3-year minimum data rule
        COUNT(stock_ret_3d) OVER w_4y AS count_corr_days
    FROM three_day_calculation
    WINDOW w_4y AS (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 1007 PRECEDING AND CURRENT ROW
    )
),

-- STEP 4: Calculate Time-Series Beta
time_series_beta AS (
    SELECT 
        *,
        -- Beta = Correlation * (Stock Volatility / Market Volatility)
        rho_4y * (vol_stock_1y / NULLIF(vol_mkt_1y, 0)) AS beta_ts
    FROM correlation_calculation
),

-- STEP 5: Calculate final metrics, aggregate to quarter-end, and rank rows
quarter_aggregation AS (
    SELECT 
        ticker,
        date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(QUARTER FROM date) AS quarter,
        (EXTRACT(YEAR FROM date) * 4) + EXTRACT(QUARTER FROM date) AS absolute_quarter,
        
        -- Underlying Metrics
        vol_stock_1y,
        vol_mkt_1y,
        stock_ret_3d,
        mkt_ret_3d,
        rho_4y,
        count_corr_days,
        beta_ts,
        
        -- Shrinkage Beta: Vasicek's bayesian shrinkage toward 1.0
        (0.6 * beta_ts + 0.4) AS beta_final,
        
        -- Betting Against Beta (BAB) Score
        -1 * ((0.6 * beta_ts) + 0.4) AS bab_score,
        
        -- Rank to extract the last trading day of each quarter
        ROW_NUMBER() OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date) 
            ORDER BY date DESC
        ) AS rn
    FROM time_series_beta
),
-- STEP 6: Filter Quarter-End records and Apply Data Quality Rules
applied_dq_rules AS (
    SELECT 
        *,
        NULLIF(
            CONCAT_WS(' | ',
                -- Require at least ~3 years of data (750 days) for a valid correlation
                CASE WHEN count_corr_days < 750 THEN 'Err: Not enough data for correlation (<750 days)' ELSE NULL END,
                -- Check for null beta (usually caused by zero market volatility)
                CASE WHEN beta_ts IS NULL THEN 'beta_ts is null (check vol_mkt_1y)' ELSE NULL END
            ), 
        '') AS unqualified_reason
    FROM quarter_aggregation
    WHERE rn = 1 
)

-- STEP 7: Final Selection (Resolve final status and inject metadata)
SELECT 
    ticker,
    year,
    quarter,
    absolute_quarter,
    vol_stock_1y,
    vol_mkt_1y,
    stock_ret_3d,
    mkt_ret_3d,
    rho_4y,
    count_corr_days,
    beta_ts,
    beta_final,
    bab_score,
    
    -- Resolve Final Status Here
    CASE 
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason
    
    -- Auto-generated audit columns
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM applied_dq_rules