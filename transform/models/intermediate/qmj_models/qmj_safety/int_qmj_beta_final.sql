{{
    config(
        materialized="table",
        tags=["intermediate", "qmj_beta_final"],
        order_by=["ticker", "date"],
    )
}}

{% set audit_cols = get_audit_columns("intermediate") %}

-- STEP 1: Extract qualified volatility data and remove ephemeral DQ columns
with
    daily_data_raw as (
        select * from {{ ref("int_qmj_beta_vol") }} where status = 'qualified'
    ),

    daily_data_cleaned as (
        -- Exclude inherited status and reason columns to avoid conflicts
        select *
        from
            TABLE(
                exclude_columns(
                    input => TABLE(daily_data_raw),
                    columns => DESCRIPTOR(status, unqualified_reason)
                )
            )
    ),

    -- STEP 2: Calculate 3-day overlapping returns
    three_day_calculation as (
        select
            *,
            SUM(stock_ret) over (
                partition by ticker
                order by date
                rows between 2 PRECEDING and CURRENT ROW
            ) as stock_ret_3d,
            SUM(mkt_ret) over (
                partition by ticker
                order by date
                rows between 2 PRECEDING and CURRENT ROW
            ) as mkt_ret_3d
        from daily_data_cleaned
    ),

    -- STEP 3: Calculate 4-year rolling correlation (1008 trading days)
    correlation_calculation as (
        select
            *,
            -- Calculate Pearson correlation coefficient over 4 years
            CORR(stock_ret_3d, mkt_ret_3d) over w_4y as rho_4y,

            -- Count actual data points to enforce the 3-year minimum data rule
            COUNT(stock_ret_3d) over w_4y as count_corr_days
        from three_day_calculation
        window
            w_4y as (
                partition by ticker
                order by date
                rows between 1007 PRECEDING and CURRENT ROW
            )
    ),

    -- STEP 4: Calculate Time-Series Beta
    time_series_beta as (
        select
            *,
            -- Beta = Correlation * (Stock Volatility / Market Volatility)
            rho_4y * (vol_stock_1y / NULLIF(vol_mkt_1y, 0)) as beta_ts
        from correlation_calculation
    ),

    -- STEP 5: Calculate final metrics, aggregate to quarter-end, and rank rows
    quarter_aggregation as (
        select
            ticker,
            date,
            EXTRACT(YEAR from date) as year,
            EXTRACT(QUARTER from date) as quarter,
            (EXTRACT(YEAR from date) * 4)
            + EXTRACT(QUARTER from date) as absolute_quarter,

            -- Underlying Metrics
            vol_stock_1y,
            vol_mkt_1y,
            stock_ret_3d,
            mkt_ret_3d,
            rho_4y,
            count_corr_days,
            beta_ts,

            -- Shrinkage Beta: Vasicek's bayesian shrinkage toward 1.0
            (0.6 * beta_ts + 0.4) as beta_final,

            -- Betting Against Beta (BAB) Score
            -1 * ((0.6 * beta_ts) + 0.4) as bab_score,

            -- Rank to extract the last trading day of each quarter
            ROW_NUMBER() over (
                partition by ticker, EXTRACT(YEAR from date), EXTRACT(QUARTER from date)
                order by date DESC
            ) as rn
        from time_series_beta
    ),
    -- STEP 6: Filter Quarter-End records and Apply Data Quality Rules
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Require at least ~3 years of data (750 days) for a valid
                    -- correlation
                    case
                        when count_corr_days < 750
                        then 'Err: Not enough data for correlation (<750 days)'
                        else NULL
                    end,
                    -- Check for null beta (usually caused by zero market volatility)
                    case
                        when beta_ts is NULL
                        then 'beta_ts is null (check vol_mkt_1y)'
                        else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from quarter_aggregation
        where rn = 1
    )

-- STEP 7: Final Selection (Resolve final status and inject metadata)
select
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
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    {% for col in audit_cols %}, {{ col.expr }} as {{ col.alias }} {% endfor %}

from applied_dq_rules
