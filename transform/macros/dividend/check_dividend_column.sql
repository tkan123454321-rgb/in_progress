{% macro check_dividend_columns() %}

    {% set indicators = get_dividend_columns() %}

    NULLIF(
        CONCAT_WS(
            ' | ',

            -- AUTO-CHECK FOR NON-NEGATIVE VALUES (MUST BE POSITIVE)
            {% for ind in indicators %}
                {% if ind.must_be_positive %}
                    case
                        when COALESCE({{ ind.alias }}, 0) < 0
                        then '{{ ind.alias }} cannot be negative'
                        else NULL
                    end,
                {% endif %}
            {% endfor %}

            NULL
        ),
        ''
    )

{% endmacro %}
