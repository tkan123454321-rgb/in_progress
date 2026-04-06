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
    {% endif %}
{% endmacro %}