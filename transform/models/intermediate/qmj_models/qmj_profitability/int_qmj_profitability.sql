{{ config(materialized = 'table') }}

WITH calc_wc AS (
    SELECT *,
        (
            total_equity - minority_interest - preferred_stock
        ) AS book_equity,
        (
            current_assets - current_liabilities - cash_and_equivalents + short_term_debt + income_taxes_payable
        ) AS working_capital
    FROM {{ ref('int_ttm_metrics') }}
    WHERE ttm_status = 'valid_ttm'
),
calc_delta AS (
    SELECT *,
        (
            working_capital - LAG(working_capital, 4) OVER (
                PARTITION BY ticker
                ORDER BY year,
                    quarter
            )
        ) AS delta_working_capital,
        (
            absolute_quarter - LAG(absolute_quarter, 4) OVER (
                PARTITION BY ticker
                ORDER BY year,
                    quarter
            )
        ) AS quarter_gap_wc
    FROM calc_wc
),
calc_all_profitability AS (
    SELECT *,
        (gross_profit_ttm / NULLIF(total_assets, 0)) AS gpoa,
        (net_income_parent_ttm / NULLIF(book_equity, 0)) AS roe,
        (net_income_ttm / NULLIF(total_assets, 0)) AS roa,
        (gross_profit_ttm / NULLIF(net_revenue_ttm, 0)) AS gmar,
        (
            (
                net_income_ttm + depreciation_ttm - delta_working_capital - capex_ttm
            ) / NULLIF(total_assets, 0)
        ) AS cfoa,
        (
            (depreciation_ttm - delta_working_capital) / NULLIF(total_assets, 0)
        ) AS acc
    FROM calc_delta
),
-- 🔥 Đưa Macro check xuống CTE này để SQL kịp nhận diện các cột gpoa, roe...
apply_dq AS (
    SELECT *,
        {{ check_qmj_column('profitability') }} AS unqualified_reason
    FROM calc_all_profitability
)
SELECT ticker,
    year,
    quarter,
    absolute_quarter,
    gpoa,
    roe,
    roa,
    gmar,
    cfoa,
    acc,
    -- Bỏ dấu phẩy ở cuổi hàm CASE để chuẩn bị cho vòng lặp Audit columns
    CASE
        WHEN unqualified_reason IS NULL THEN 'qualified'
        ELSE 'unqualified'
    END AS status,
    unqualified_reason
    
    {% set audit_cols = get_audit_columns('intermediate') %}
    {% for col in audit_cols %}
    , {{ col.expr }} AS {{ col.alias }}
    {% endfor %}

FROM apply_dq