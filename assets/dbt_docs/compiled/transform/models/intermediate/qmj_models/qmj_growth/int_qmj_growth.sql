


-- STEP 1: Extract valid TTM metrics and calculate Book Equity & Working Capital
with
    base_metrics as (
        select
            *,
            (total_equity - minority_interest - preferred_stock) as book_equity,
            (
                current_assets
                - current_liabilities
                - cash_and_equivalents
                + short_term_debt
                + income_taxes_payable
            ) as working_capital
        from "lakehouse_main"."intermediate"."int_ttm_metrics"
        where ttm_status = 'valid_ttm'
    ),

    -- STEP 2: Calculate the 1-year change in Working Capital
    calc_delta_wc as (
        select
            *,
            (
                working_capital - LAG(working_capital, 4) over w_ticker
            ) as delta_working_capital
        from base_metrics
        window w_ticker as (partition by ticker order by absolute_quarter)
    ),

    -- STEP 3: Derive Cash Flow from Operations (CFO) if not directly available
    calc_cfo as (
        select
            *,
            (
                net_income_ttm + depreciation_ttm - delta_working_capital - capex_ttm
            ) as derived_cfo_ttm
        from calc_delta_wc
    ),
    -- STEP 4: Prepare historical data points (Lags) for Growth calculations
    prepare_history as (
        select
            *,
            -- Continuity Check: Gap between current quarter and 16 quarters (4 years)
            -- ago
            (
                absolute_quarter - LAG(absolute_quarter, 16) over w_ticker
            ) as quarter_gap_16,

            -- 1 Year Ago (Current Hurdle)
            LAG(total_assets, 4) over w_ticker as assets_t_minus_1_yr,
            LAG(book_equity, 4) over w_ticker as equity_t_minus_1_yr,

            -- 3 Years Ago (Historical Surplus)
            LAG(gross_profit_ttm, 12) over w_ticker as gp_past,
            LAG(net_income_parent_ttm, 12) over w_ticker as ni_parent_past,
            LAG(net_income_ttm, 12) over w_ticker as ni_past,
            LAG(derived_cfo_ttm, 12) over w_ticker as derived_cfo_past,
            LAG(net_revenue_ttm, 12) over w_ticker as rev_past,
            LAG(total_assets, 12) over w_ticker as assets_past,
            LAG(book_equity, 12) over w_ticker as equity_past,
            LAG(risk_free_rate, 12) over w_ticker as rf_past,

            -- 4 Years Ago (Historical Hurdle)
            LAG(total_assets, 16) over w_ticker as assets_past_minus_1_yr,
            LAG(book_equity, 16) over w_ticker as equity_past_minus_1_yr

        from calc_cfo
        window w_ticker as (partition by ticker order by absolute_quarter)
    ),
    -- STEP 5: Calculate the 5 core Growth metrics
    calc_growth_scores as (
        select
            *,
            -- 1. Delta Gross Profits over Assets (GPOA)
            (
                (gross_profit_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr))
                - (gp_past - ((rf_past / 100) * assets_past_minus_1_yr))
            )
            / NULLIF(assets_past, 0) as delta_gpoa,

            -- 2. Delta Return on Equity (ROE)
            (
                (net_income_parent_ttm - ((risk_free_rate / 100) * equity_t_minus_1_yr))
                - (ni_parent_past - ((rf_past / 100) * equity_past_minus_1_yr))
            )
            / NULLIF(equity_past, 0) as delta_roe,

            -- 3. Delta Cash Flow over Assets (CFOA)
            (
                (derived_cfo_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr))
                - (derived_cfo_past - ((rf_past / 100) * assets_past_minus_1_yr))
            )
            / NULLIF(assets_past, 0) as delta_cfoa,

            -- 4. Delta Return on Assets (ROA)
            (
                (net_income_ttm - ((risk_free_rate / 100) * assets_t_minus_1_yr))
                - (ni_past - ((rf_past / 100) * assets_past_minus_1_yr))
            )
            / NULLIF(assets_past, 0) as delta_roa,

            -- 5. Delta Gross Margin (GMAR)
            (gross_profit_ttm - gp_past) / NULLIF(rev_past, 0) as delta_gmar

        from prepare_history
    ),
    -- STEP 6: Apply Data Quality Rules specific to Growth factors
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Ensure full 16-quarter history without gaps
                    case
                        when quarter_gap_16 is NULL or quarter_gap_16 != 16
                        then 'Err: Broken history for Growth (Gap != 16)'
                        else NULL
                    end,

                    -- Check for null values in the final metrics
                    case
                        when delta_gpoa is NULL then 'delta_gpoa is null' else NULL
                    end,
                    case when delta_roe is NULL then 'delta_roe is null' else NULL end,
                    case when delta_roa is NULL then 'delta_roa is null' else NULL end,
                    case
                        when delta_cfoa is NULL then 'delta_cfoa is null' else NULL
                    end,
                    case when delta_gmar is NULL then 'delta_gmar is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from calc_growth_scores
    )

-- STEP 7: Final Selection , injecting audit columns, and resolving final status
select
    ticker,
    year,
    quarter,
    absolute_quarter,
    delta_gpoa,
    delta_roe,
    delta_cfoa,
    delta_roa,
    delta_gmar,

    -- Resolve Final Status
    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

    -- Auto-generated audit columns
    , CAST(from_iso8601_timestamp('2026-05-06T08:48:04.916793+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , 'd5f144b3-ec78-4c38-93a0-f54d53bb219b' as int_invocation_id 

from applied_dq_rules