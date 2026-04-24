{% macro check_dividend_columns() %}

    {% set indicators = get_dividend_columns() %}

    NULLIF(
        CONCAT_WS(' | ',

            --AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
            {% for ind in indicators %}
                {% if ind.must_be_positive %}
            CASE WHEN COALESCE({{ ind.alias }}, 0) < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                {% endif %}
            {% endfor %}
            
            NULL   
        ), 
    '')

{% endmacro %}