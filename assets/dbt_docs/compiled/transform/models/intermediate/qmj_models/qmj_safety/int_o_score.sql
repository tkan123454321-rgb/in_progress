



-- STEP 1: Extract valid TTM metrics and calculate base financial components
with
    base_metrics as (
        select
            *,
            -- Book Equity
            (total_equity - minority_interest - preferred_stock) as book_equity,

            -- Adjusted Assets: AT + 0.1 * (Market Equity - Book Equity)
            (
                total_assets
                + 0.1
                * (market_cap - (total_equity - minority_interest - preferred_stock))
            ) as adj_asset,

            -- Total Debt: Short Term + Long Term
            (short_term_debt + long_term_debt) as total_debt

        from "lakehouse_main"."intermediate"."int_ttm_metrics"
        where ttm_status = 'valid_ttm'
    ),

    -- STEP 2: Prepare lagged metrics for Year-over-Year comparison
    prep_lags as (
        select
            *,
            -- Retrieve Net Income from 1 year ago (4 quarters prior)
            LAG(net_income_ttm, 4) over w_window as net_income_past,

            -- Check data continuity (must exactly equal 4 for a 1-year gap)
            (absolute_quarter - LAG(absolute_quarter, 4) over w_window) as quarter_gap_4

        from base_metrics
        window w_window as (partition by ticker order by absolute_quarter)
    ),

    -- STEP 3: Calculate the 9 core variables of the Ohlson O-Score model
    calc_ohlson_vars as (
        select
            *,
            -- Used the pre-calculated adj_asset to simplify log_size
            LN(NULLIF(adj_asset, 0) / NULLIF(cpi_index, 0)) as log_size,
            (total_debt / NULLIF(adj_asset, 0)) as tlta,
            ((current_assets - current_liabilities) / NULLIF(adj_asset, 0)) as wcta,
            (current_liabilities / NULLIF(current_assets, 0)) as clca,
            case when total_liabilities > total_assets then 1 else 0 end as oeneg,
            (net_income_ttm / NULLIF(total_assets, 0)) as nita,
            (profit_before_tax_ttm / NULLIF(total_liabilities, 0)) as futl,
            case
                when GREATEST(net_income_ttm, net_income_past) < 0 then 1 else 0
            end as intwo,
            (net_income_ttm - net_income_past)
            / NULLIF(ABS(net_income_ttm) + ABS(net_income_past), 0) as chin
        from prep_lags
    ),

    -- STEP 4: Calculate the final Ohlson O-Score
    calc_o_score as (
        select
            *,
            -- The final linear equation for default probability
            - (
                -1.32
                - 0.407 * log_size
                + 6.03 * tlta
                - 1.43 * wcta
                + 0.076 * clca
                - 1.72 * oeneg
                - 2.37 * nita
                - 1.83 * futl
                + 0.285 * intwo
                - 0.521 * chin
            ) as ohlson_o_score
        from calc_ohlson_vars
    ),

    -- STEP 5: Apply Data Quality Rules specific to Ohlson O-Score
    applied_dq_rules as (
        select
            *,
            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- Continuity Check
                    case
                        when quarter_gap_4 is NULL or quarter_gap_4 != 4
                        then 'Err: Broken history for O-Score (Gap != 4)'
                        else NULL
                    end,

                    -- Null Checks for all 9 components
                    case
                        when log_size is NULL
                        then 'log_size is null (Check Assets/CPI)'
                        else NULL
                    end,
                    case when tlta is NULL then 'tlta is null' else NULL end,
                    case when wcta is NULL then 'wcta is null' else NULL end,
                    case when clca is NULL then 'clca is null' else NULL end,
                    case when oeneg is NULL then 'oeneg is null' else NULL end,
                    case when nita is NULL then 'nita is null' else NULL end,
                    case when futl is NULL then 'futl is null' else NULL end,
                    case when intwo is NULL then 'intwo is null' else NULL end,
                    case when chin is NULL then 'chin is null' else NULL end
                ),
                ''
            ) as unqualified_reason
        from calc_o_score
    )

select
    ticker,
    year,
    quarter,
    absolute_quarter,
    log_size,
    tlta,
    wcta,
    clca,
    oeneg,
    nita,
    futl,
    intwo,
    chin,
    ohlson_o_score,

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,

    unqualified_reason

    , CAST(from_iso8601_timestamp('2026-05-06T08:53:01.583492+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as int_updated_at , '4c6d9271-375a-4d96-926e-49714c96b216' as int_invocation_id 
from applied_dq_rules