{% macro check_fundamental_columns(fundamental_type) %}

    {% set indicators = get_fundamental_columns(fundamental_type) %}

    {% if fundamental_type == "fundamental_1" %}
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. Check for NULLs in mandatory columns (is_mandatory = True)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is mandatory but null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- 2. Check for non-negative values (must_be_positive = True)
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

    {% elif fundamental_type == "fundamental_2" %}
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. Rule: Check for valid stock exchanges
                case
                    when UPPER(exchange) not in ('UPCOM', 'HSX', 'HOSE', 'HNX')
                    then 'invalid_exchange'
                    else NULL
                end,

                -- 2. Rule: Check listing status (Must be actively listed)
                case when is_listing = FALSE then 'delisted_or_inactive' else NULL end,

                -- 3. Check for NULLs in mandatory columns
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is mandatory but null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                NULL  -- Trick to prevent trailing pipe errors
            ),
            ''
        )
    {% elif fundamental_type == "fundamental_quarter" %}
        NULLIF(
            CONCAT_WS(
                ' | ',
                -- 1. AUTO-CHECK NULLS FOR MANDATORY COLUMNS (is_mandatory)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                        case
                            when {{ ind.alias }} is NULL
                            then '{{ ind.alias }} is mandatory but null'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- 2. AUTO-CHECK FOR NON-NEGATIVE VALUES (must_be_positive)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                        case
                            when {{ ind.alias }} < 0
                            then '{{ ind.alias }} cannot be negative'
                            else NULL
                        end,
                    {% endif %}
                {% endfor %}

                -- 3. CUSTOM BUSINESS LOGIC CHECKS (If needed)
                -- Example: Market Cap must be greater than 0
                case when market_cap = 0 then 'Err: Market Cap is zero' else NULL end
            ),
            ''
        )
    {% endif %}

{% endmacro %}
