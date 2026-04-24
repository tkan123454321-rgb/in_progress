

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
    {% elif factor_type == 'qmj_final' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- CHECK ĐỦ 3 CHÂN KIỀNG CỦA CHỈ SỐ CHẤT LƯỢNG
                CASE WHEN qmj_profitability_score IS NULL THEN 'Missing Profitability Score' ELSE NULL END,
                CASE WHEN qmj_growth_score IS NULL THEN 'Missing Growth Score' ELSE NULL END,
                CASE WHEN qmj_safety_score IS NULL THEN 'Missing Safety Score' ELSE NULL END
             ), 
        '')
    {% endif %}
{% endmacro %}