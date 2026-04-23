{% macro check_diamond_score_column(factor_type) %}
    {% if factor_type == 'diamond' %}
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN qmj_score IS NULL THEN 'Missing Quality (QMJ)' ELSE NULL END,
                CASE WHEN z_value IS NULL THEN 'Missing Value (B/P)' ELSE NULL END,
                CASE WHEN z_momentum IS NULL THEN 'Missing Momentum (12-1)' ELSE NULL END
            ), 
        '')
    {% endif %}
{% endmacro %}