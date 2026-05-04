{% macro check_financial_reports(report_type) %}

    {% set indicators = get_financial_reports_column(report_type) %}

    {% if report_type == "income_statement" %}
        NULLIF(
            CONCAT_WS(
                ' | ',

                -- PART 1: AUTO-CHECK NULLS FOR MANDATORY COLUMNS
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- PART 1.5: AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                        case
                            when {{ ind.alias }} < 0
                            then '{{ ind.alias }} cannot be negative'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- PART 2: MATHEMATICAL CHECKS (BUSINESS LOGIC)
                -- 3 = (1) - (2): Check Net Revenue
                case
                    when
                        ABS(
                            COALESCE(net_revenue, 0) - (
                                COALESCE(gross_revenue, 0)
                                - COALESCE(revenue_deduction, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 3 (Net Rev) != (1) - (2)'
                    else NULL
                end,

                -- 5 = (3) - (4): Check Gross Profit
                case
                    when
                        ABS(
                            COALESCE(gross_profit, 0)
                            - (COALESCE(net_revenue, 0) - COALESCE(cogs, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 5 (Gross Profit) != (3) - (4)'
                    else NULL
                end,

                -- 11 = (5) + (6) - (7) + (8) - (9) - (10): Check Net Operating Profit
                case
                    when
                        ABS(
                            COALESCE(operating_profit, 0) - (
                                COALESCE(gross_profit, 0)
                                + COALESCE(financial_income, 0)
                                - COALESCE(financial_expense, 0)
                                + COALESCE(affiliate_profit, 0)
                                - COALESCE(selling_expense, 0)
                                - COALESCE(admin_expense, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 11 (Operating Profit) != (5)+(6)-(7)+(8)-(9)-(10)'
                    else NULL
                end,

                -- 14 = (12) - (13): Check Other Profit
                case
                    when
                        ABS(
                            COALESCE(other_profit, 0)
                            - (COALESCE(other_income, 0) - COALESCE(other_expense, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 14 (Other Profit) != (12) - (13)'
                    else NULL
                end,

                -- 15 = (11) + (14): Check Total Pre-tax Profit
                case
                    when
                        ABS(
                            COALESCE(profit_before_tax, 0) - (
                                COALESCE(operating_profit, 0)
                                + COALESCE(other_profit, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 15 (Pre-tax Profit) != (11) + (14)'
                    else NULL
                end,

                -- 18 = (16) + (17): Check Total Tax Expense
                case
                    when
                        ABS(
                            COALESCE(income_tax_expense, 0)
                            - (COALESCE(current_tax, 0) + COALESCE(deferred_tax, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 18 (Tax Expense) != (16) + (17)'
                    else NULL
                end,

                -- 19 = (15) - (18): Check Net Income (Corporate)
                case
                    when
                        ABS(
                            COALESCE(net_income, 0) - (
                                COALESCE(profit_before_tax, 0)
                                - COALESCE(income_tax_expense, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 19 (Net Income) != (15) - (18)'
                    else NULL
                end,

                -- 21 = (19) - (20): Check Net Income to Parent Company Shareholders
                case
                    when
                        ABS(
                            COALESCE(net_income_parent, 0)
                            - (COALESCE(net_income, 0) - COALESCE(minority_interest, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_revenue, 0))
                    then 'Err: ID 21 (Net Income Parent) != (19) - (20)'
                    else NULL
                end
            ),
            ''
        )

    {% elif report_type == "cash_flow_indirect" %}
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- PART 1: AUTO-CHECK NULLS FOR MANDATORY COLUMNS
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- PART 1.5: AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                        case
                            when {{ ind.alias }} < 0
                            then '{{ ind.alias }} cannot be negative'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- PART 2: MATHEMATICAL CHECKS (BUSINESS LOGIC)
                -- Equation 1: ID 4 = ID 104 + ID 212 + ID 311
                case
                    when
                        ABS(
                            COALESCE(net_cash_flow, 0)
                            - (COALESCE(cfo, 0) + COALESCE(cfi, 0) + COALESCE(cff, 0))
                        )
                        > 0.01 * ABS(COALESCE(net_cash_flow, 0))
                    then 'Err: ID 4 (Net CF) != ID 104 + 212 + 311'
                    else NULL
                end,

                -- Equation 2: ID 7 = ID 5 + ID 4 + ID 6
                case
                    when
                        ABS(
                            COALESCE(ending_cash, 0) - (
                                COALESCE(beginning_cash, 0)
                                + COALESCE(net_cash_flow, 0)
                                + COALESCE(exchange_rate_effect, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(ending_cash, 0))
                    then 'Err: ID 7 (Ending Cash) != ID 5 + 4 + 6'
                    else NULL
                end

            ),
            ''
        )

    {% elif report_type == "balance_sheet" %}
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- PART 1: AUTO-CHECK NULLS FOR MANDATORY COLUMNS
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- PART 1.5: AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                        case
                            when {{ ind.alias }} < 0
                            then '{{ ind.alias }} cannot be negative'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- PART 2: MATHEMATICAL CHECKS (BUSINESS LOGIC)
                -- Check 1: Global Balance Sheet Equation
                case
                    when
                        ABS(COALESCE(total_assets, 0) - COALESCE(total_capital, 0))
                        > 0.01 * ABS(COALESCE(total_assets, 0))
                    then 'Err: Lệch Cân đối (Tài sản != Nguồn vốn)'
                    else NULL
                end,

                -- Check 2: Asset Structure
                case
                    when
                        ABS(
                            COALESCE(total_assets, 0) - (
                                COALESCE(current_assets, 0)
                                + COALESCE(long_term_assets, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(total_assets, 0))
                    then 'Err: Tổng Tài sản != Ngắn hạn + Dài hạn'
                    else NULL
                end,

                -- Check 3: Capital/Equity Structure
                case
                    when
                        ABS(
                            COALESCE(total_capital, 0) - (
                                COALESCE(total_liabilities, 0)
                                + COALESCE(total_equity, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(total_capital, 0))
                    then 'Err: Tổng Nguồn vốn != Nợ phải trả + Vốn CSH'
                    else NULL
                end,

                -- Check 4: Liabilities Structure
                case
                    when
                        ABS(
                            COALESCE(total_liabilities, 0) - (
                                COALESCE(current_liabilities, 0)
                                + COALESCE(long_term_liabilities, 0)
                            )
                        )
                        > 0.01 * ABS(COALESCE(total_liabilities, 0))
                    then 'Err: Nợ phải trả != Nợ ngắn hạn + Nợ dài hạn'
                    else NULL
                end

            ),
            ''
        )

    {% elif report_type == "historical_quotes" %}
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. AUTO-CHECK NULLS FOR MANDATORY COLUMNS (price_basic,
                -- price_close, ticker...)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is mandatory but null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- 2. AUTO-CHECK FOR NON-NEGATIVE VALUES (prices, volume)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                        case
                            when {{ ind.alias }} < 0
                            then '{{ ind.alias }} cannot be negative'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}
                NULL
            ),
            ''
        )
    {% endif %}

{% endmacro %}
