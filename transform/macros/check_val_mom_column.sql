{% macro check_value_and_momentum_column(factor_type) %}

    {% if factor_type == 'value' %}
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN gap_6m IS NULL OR gap_6m != 2 THEN 'Err: Missing 6-month lag for Book Equity (Gap != 2)' ELSE NULL END,
                CASE WHEN book_equity_6m_lag IS NULL THEN 'Err: Lagged Book Equity is null' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'momentum' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- Bắt buộc phải có cả Gap 1m và Gap 12m
                CASE WHEN gap_12m IS NULL OR gap_12m != 12 THEN 'Err: Missing 12-month lag for Price (Gap != 12)' ELSE NULL END,
                
                -- Đảm bảo giá không bị Null hoặc bằng 0 (để tránh lỗi chia cho 0)
                CASE WHEN price_t_1 IS NULL THEN 'Err: Lagged Price 1m is null' ELSE NULL END,
                CASE WHEN price_t_12 IS NULL OR price_t_12 = 0 THEN 'Err: Lagged Price 12m is null or zero' ELSE NULL END
            ), 
        '')
    {% endif %}

{% endmacro %}

{% macro check_value_and_momentum_z_score_column(factor_type) %}

    {% if factor_type == 'value_momentum_z' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- CHECK NULL CHO 2 Z-SCORE QUAN TRỌNG NHẤT
                CASE WHEN z_value IS NULL THEN 'z_value is null' ELSE NULL END,
                CASE WHEN z_momentum IS NULL THEN 'z_momentum is null' ELSE NULL END
             ), 
        '')
    {% endif %}

{% endmacro %}