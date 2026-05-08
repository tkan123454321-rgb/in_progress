



-- STEP 1: Extract valid TTM metrics and calculate EBIT & Working Capital
with
    base_metrics as (
        select
            *,
            -- EBIT = Profit Before Tax + Interest Expense
            (profit_before_tax_ttm + interest_expense_ttm) as ebit_ttm,

            -- Working Capital = Current Assets - Current Liabilities
            (current_assets - current_liabilities) as working_capital
        from "lakehouse_main"."intermediate"."int_ttm_metrics"
        where ttm_status = 'valid_ttm'
    ),

    -- STEP 2: Calculate the 5 core ratios of the Altman Z-Score model (X1 to X5)
    calc_z_components as (
        select
            *,
            -- Standardize by Total Assets (AT)
            (working_capital / NULLIF(total_assets, 0)) as x1_wc_at,
            (retained_earnings / NULLIF(total_assets, 0)) as x2_re_at,
            (ebit_ttm / NULLIF(total_assets, 0)) as x3_ebit_at,
            (market_cap / NULLIF(total_assets, 0)) as x4_me_at,
            (net_revenue_ttm / NULLIF(total_assets, 0)) as x5_sale_at
        from base_metrics
    ),
    -- STEP 3: Apply the standard Altman Z-Score weighting formula
    calc_z_score as (
        select
            *,
            (
                1.2 * x1_wc_at
                + 1.4 * x2_re_at
                + 3.3 * x3_ebit_at
                + 0.6 * x4_me_at
                + 1.0 * x5_sale_at
            ) as altman_z_score
        from calc_z_components
    ),

    -- STEP 4: Apply Data Quality Rules specific to Altman Z-Score
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Altman model uses point-in-time data, no lags required. Just
                    -- check components.
                    case when working_capital is NULL then 'wc is null' else NULL end,
                    case when retained_earnings is NULL then 're is null' else NULL end,
                    case when ebit_ttm is NULL then 'ebit is null' else NULL end,
                    case
                        when market_cap is NULL then 'market_cap is null' else NULL
                    end,
                    case
                        when net_revenue_ttm is NULL then 'revenue is null' else NULL
                    end
                ),
                ''
            ) as unqualified_reason
        from calc_z_score
    )

-- STEP 5: Final Selection and Status Resolution
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    x1_wc_at,
    x2_re_at,
    x3_ebit_at,
    x4_me_at,
    x5_sale_at,
    altman_z_score,

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    unqualified_reason

    , CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , '4ff423e7-7675-4eec-a090-58bdf9560b12' as int_invocation_id 

from applied_dq_rules