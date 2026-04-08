{% macro dq_check_financial_reports(report_type) %}

    {% set indicators = get_financial_reports_column(report_type) %}

    {% if report_type == 'income_statement' %}
        NULLIF(
            CONCAT_WS(' | ',
            
                -- PHẦN 1: TỰ ĐỘNG CHECK NULL CÁC CỘT BẮT BUỘC
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- PHẦN 1.5: TỰ ĐỘNG CHECK LUẬT KHÔNG ĐƯỢC ÂM (MUST BE POSITIVE)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- PHẦN 2: CHECK TOÁN HỌC (BUSINESS LOGIC)
                -- 3 = (1) - (2): Check Doanh thu thuần
                CASE WHEN ABS(COALESCE(net_revenue, 0) - (COALESCE(gross_revenue, 0) - COALESCE(revenue_deduction, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 3 (Net Rev) != (1) - (2)' ELSE NULL END,

                -- 5 = (3) - (4): Check Lợi nhuận gộp
                CASE WHEN ABS(COALESCE(gross_profit, 0) - (COALESCE(net_revenue, 0) - COALESCE(cogs, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 5 (Gross Profit) != (3) - (4)' ELSE NULL END,

                -- 11 = (5) + (6) - (7) + (8) - (9) - (10): Check Lợi nhuận thuần từ HĐKD
                CASE WHEN ABS(COALESCE(operating_profit, 0) - (
                    COALESCE(gross_profit, 0) + COALESCE(financial_income, 0) - COALESCE(financial_expense, 0) 
                    + COALESCE(affiliate_profit, 0) - COALESCE(selling_expense, 0) - COALESCE(admin_expense, 0)
                )) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 11 (Operating Profit) != (5)+(6)-(7)+(8)-(9)-(10)' ELSE NULL END,

                -- 14 = (12) - (13): Check Lợi nhuận khác
                CASE WHEN ABS(COALESCE(other_profit, 0) - (COALESCE(other_income, 0) - COALESCE(other_expense, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 14 (Other Profit) != (12) - (13)' ELSE NULL END,

                -- 15 = (11) + (14): Check Tổng lợi nhuận trước thuế
                CASE WHEN ABS(COALESCE(profit_before_tax, 0) - (COALESCE(operating_profit, 0) + COALESCE(other_profit, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 15 (Pre-tax Profit) != (11) + (14)' ELSE NULL END,

                -- 18 = (16) + (17): Check Tổng chi phí thuế
                CASE WHEN ABS(COALESCE(income_tax_expense, 0) - (COALESCE(current_tax, 0) + COALESCE(deferred_tax, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 18 (Tax Expense) != (16) + (17)' ELSE NULL END,

                -- 19 = (15) - (18): Check Lợi nhuận sau thuế DN
                CASE WHEN ABS(COALESCE(net_income, 0) - (COALESCE(profit_before_tax, 0) - COALESCE(income_tax_expense, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 19 (Net Income) != (15) - (18)' ELSE NULL END,

                -- 21 = (19) - (20): Check Lợi nhuận sau thuế Cổ đông công ty mẹ
                CASE WHEN ABS(COALESCE(net_income_parent, 0) - (COALESCE(net_income, 0) - COALESCE(minority_interest, 0))) > 0.01 * ABS(COALESCE(net_revenue, 0)) 
                THEN 'Err: ID 21 (Net Income Parent) != (19) - (20)' ELSE NULL END
            ), 
        '')
        
    {% elif report_type == 'cash_flow_indirect' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- PHẦN 1: TỰ ĐỘNG CHECK NULL CÁC CỘT BẮT BUỘC
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- PHẦN 1.5: TỰ ĐỘNG CHECK LUẬT KHÔNG ĐƯỢC ÂM (MUST BE POSITIVE)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- PHẦN 2: CHECK TOÁN HỌC (BUSINESS LOGIC)

                -- Phương trình 1: ID 4 = ID 104 + ID 212 + ID 311 
                CASE WHEN ABS(COALESCE(net_cash_flow, 0) - (COALESCE(cfo, 0) + COALESCE(cfi, 0) + COALESCE(cff, 0))) > 0.01 * ABS(COALESCE(net_cash_flow, 0)) 
                THEN 'Err: ID 4 (Net CF) != ID 104 + 212 + 311' ELSE NULL END,

                -- Phương trình 2: ID 7 = ID 5 + ID 4 + ID 6
                CASE WHEN ABS(COALESCE(ending_cash, 0) - (COALESCE(beginning_cash, 0) + COALESCE(net_cash_flow, 0) + COALESCE(exchange_rate_effect, 0))) > 0.01 * ABS(COALESCE(ending_cash, 0)) 
                THEN 'Err: ID 7 (Ending Cash) != ID 5 + 4 + 6' ELSE NULL END

            ), 
        '')
        
    {% elif report_type == 'balance_sheet' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- PHẦN 1: TỰ ĐỘNG CHECK NULL CÁC CỘT BẮT BUỘC
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- PHẦN 1.5: TỰ ĐỘNG CHECK LUẬT KHÔNG ĐƯỢC ÂM (MUST BE POSITIVE)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- PHẦN 2: CHECK TOÁN HỌC (BUSINESS LOGIC)

                -- Ải 1: Cân đối Kế toán Toàn cục 
                CASE WHEN ABS(COALESCE(total_assets, 0) - COALESCE(total_capital, 0)) > 0.01 * ABS(COALESCE(total_assets, 0)) 
                THEN 'Err: Lệch Cân đối (Tài sản != Nguồn vốn)' ELSE NULL END,

                -- Ải 2: Cấu trúc Tài sản 
                CASE WHEN ABS(COALESCE(total_assets, 0) - (COALESCE(current_assets, 0) + COALESCE(long_term_assets, 0))) > 0.01 * ABS(COALESCE(total_assets, 0)) 
                THEN 'Err: Tổng Tài sản != Ngắn hạn + Dài hạn' ELSE NULL END,

                -- Ải 3: Cấu trúc Nguồn vốn 
                CASE WHEN ABS(COALESCE(total_capital, 0) - (COALESCE(total_liabilities, 0) + COALESCE(total_equity, 0))) > 0.01 * ABS(COALESCE(total_capital, 0)) 
                THEN 'Err: Tổng Nguồn vốn != Nợ phải trả + Vốn CSH' ELSE NULL END,

                -- Ải 4: Cấu trúc Nợ phải trả
                CASE WHEN ABS(COALESCE(total_liabilities, 0) - (COALESCE(current_liabilities, 0) + COALESCE(long_term_liabilities, 0))) > 0.01 * ABS(COALESCE(total_liabilities, 0)) 
                THEN 'Err: Nợ phải trả != Nợ ngắn hạn + Nợ dài hạn' ELSE NULL END

            ), 
        '')
    
    {% elif report_type == 'fundamental_quarter' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. TỰ ĐỘNG CHECK NULL CÁC CỘT BẮT BUỘC (is_mandatory)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is mandatory but null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- 2. TỰ ĐỘNG CHECK LUẬT KHÔNG ĐƯỢC ÂM (must_be_positive)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- 3. CHECK LOGIC RIÊNG (Nếu cần)
                -- Ví dụ: Market Cap phải lớn hơn 0 (thường là đơn vị tỷ đồng)
                CASE WHEN market_cap = 0 THEN 'Err: Market Cap is zero' ELSE NULL END
            ), 
        '')
    {% elif report_type == 'historical_quotes' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. TỰ ĐỘNG CHECK NULL CÁC CỘT BẮT BUỘC (price_basic, price_close, ticker...)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is mandatory but null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- 2. TỰ ĐỘNG CHECK LUẬT KHÔNG ĐƯỢC ÂM (các loại giá, volume)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}
                NULL
            ), 
        '')
    {% endif %}

{% endmacro %}