{% macro check_qmj_column(factor_type) %}

    {% if factor_type == 'profitability' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- ĐÃ SỬA: Bắt buộc phải có IS NULL
                CASE WHEN quarter_gap_wc IS NULL OR quarter_gap_wc != 4 
                    THEN 'Err: Missing historical quarters for Delta WC (Gap != 4)' 
                    ELSE NULL END,

                -- 2. CHECK NULL CHO 6 CHỈ SỐ QMJ
                CASE WHEN gpoa IS NULL THEN 'gpoa is null' ELSE NULL END,
                CASE WHEN roe IS NULL THEN 'roe is null' ELSE NULL END,
                CASE WHEN roa IS NULL THEN 'roa is null' ELSE NULL END,
                CASE WHEN gmar IS NULL THEN 'gmar is null' ELSE NULL END,
                CASE WHEN cfoa IS NULL THEN 'cfoa is null' ELSE NULL END,
                CASE WHEN acc IS NULL THEN 'acc is null' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'growth' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- Check gap 16 quý (4 năm) để đảm bảo không bị đứt gãy data
                CASE WHEN quarter_gap_16 IS NULL OR quarter_gap_16 != 16 
                    THEN 'Err: Broken history for Growth (Gap != 16)' 
                    ELSE NULL END,

                -- Check NULL cho 5 chỉ số tăng trưởng
                CASE WHEN delta_gpoa IS NULL THEN 'delta_gpoa is null' ELSE NULL END,
                CASE WHEN delta_roe IS NULL THEN 'delta_roe is null' ELSE NULL END,
                CASE WHEN delta_roa IS NULL THEN 'delta_roa is null' ELSE NULL END,
                CASE WHEN delta_cfoa IS NULL THEN 'delta_cfoa is null' ELSE NULL END,
                CASE WHEN delta_gmar IS NULL THEN 'delta_gmar is null' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'o_score_safety' %} 
        NULLIF(
            CONCAT_WS(' | ',
                -- Check NULL cho 8 chỉ số O-Score
                CASE WHEN quarter_gap_4 IS NULL OR quarter_gap_4 != 4 
                    THEN 'Err: Broken history for O-Score (Gap != 4)' 
                    ELSE NULL END,
                CASE WHEN log_size IS NULL THEN 'log_size is null (Check Assets/CPI)' ELSE NULL END,
                CASE WHEN tlta IS NULL THEN 'tlta is null' ELSE NULL END,
                CASE WHEN wcta IS NULL THEN 'wcta is null' ELSE NULL END,
                CASE WHEN clca IS NULL THEN 'clca is null' ELSE NULL END,
                CASE WHEN oeneg IS NULL THEN 'oeneg is null' ELSE NULL END,
                CASE WHEN nita IS NULL THEN 'nita is null' ELSE NULL END,
                CASE WHEN futl IS NULL THEN 'futl is null' ELSE NULL END,
                CASE WHEN intwo IS NULL THEN 'intwo is null' ELSE NULL END,
                CASE WHEN chin IS NULL THEN 'chin is null' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'z_score_safety' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- Altman dùng data tại chỗ, không cần Lag, chỉ cần check các biến thành phần
                CASE WHEN working_capital IS NULL THEN 'wc is null' ELSE NULL END,
                CASE WHEN retained_earnings IS NULL THEN 're is null' ELSE NULL END,
                CASE WHEN ebit_ttm IS NULL THEN 'ebit is null' ELSE NULL END,
                CASE WHEN market_cap IS NULL THEN 'market_cap is null' ELSE NULL END,
                CASE WHEN net_revenue_ttm IS NULL THEN 'revenue is null' ELSE NULL END
            ), '')
     {% elif factor_type == 'beta_volatility' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. Check phép tính Log Return có bị NULL do lỗi chia 0 không
                CASE WHEN stock_ret IS NULL THEN 'stock_ret (log return) is null' ELSE NULL END,
                CASE WHEN mkt_ret IS NULL THEN 'mkt_ret (market log return) is null' ELSE NULL END,
                
                -- 2. Check luật 120 ngày của giáo sư AQR
                CASE WHEN count_trading_days < 120 THEN 'Err: Not enough trading days (<120)' ELSE NULL END,
                
                -- 3. Check kết quả Độ lệch chuẩn cuối cùng
                CASE WHEN vol_stock_1y IS NULL THEN 'vol_stock_1y is null' ELSE NULL END,
                CASE WHEN vol_mkt_1y IS NULL THEN 'vol_mkt_1y is null' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'beta_final_calculation' %}
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN count_corr_days < 750 THEN 'Err: Not enough data for correlation (<750 days)' ELSE NULL END,
                CASE WHEN beta_ts IS NULL THEN 'beta_ts is null (check vol_mkt_1y)' ELSE NULL END,
                NULL
            ), 
        '')
    {% endif %}

{% endmacro %}

{% macro check_qmj_z_score_column(factor_type) %}

    {% if factor_type == 'profitability' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- CHECK NULL CHO 6 Z-SCORE ĐÃ TÍNH TOÁN
                CASE WHEN z_gpoa IS NULL THEN 'z_gpoa is null' ELSE NULL END,
                CASE WHEN z_roe IS NULL THEN 'z_roe is null' ELSE NULL END,
                CASE WHEN z_roa IS NULL THEN 'z_roa is null' ELSE NULL END,
                CASE WHEN z_gmar IS NULL THEN 'z_gmar is null' ELSE NULL END,
                CASE WHEN z_cfoa IS NULL THEN 'z_cfoa is null' ELSE NULL END,
                CASE WHEN z_acc IS NULL THEN 'z_acc is null' ELSE NULL END
             ), 
        '')
    {% elif factor_type == 'growth' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- CHECK NULL CHO 5 Z-SCORE ĐÃ TÍNH TOÁN
                CASE WHEN z_delta_gpoa IS NULL THEN 'z_delta_gpoa is null' ELSE NULL END,
                CASE WHEN z_delta_roe IS NULL THEN 'z_delta_roe is null' ELSE NULL END,
                CASE WHEN z_delta_roa IS NULL THEN 'z_delta_roa is null' ELSE NULL END,
                CASE WHEN z_delta_gmar IS NULL THEN 'z_delta_gmar is null' ELSE NULL END,
                CASE WHEN z_delta_cfoa IS NULL THEN 'z_delta_cfoa is null' ELSE NULL END
             ), 
        '')
    {% endif %}
{% endmacro %}