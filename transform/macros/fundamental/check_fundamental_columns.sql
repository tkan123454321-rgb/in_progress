{% macro check_fundamental_columns(fundamental_type) %}

    {% set indicators = get_fundamental_columns(fundamental_type) %}

    {% if fundamental_type == 'fundamental_1' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. Check for NULLs in mandatory columns (is_mandatory = True)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is mandatory but null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- 2. Check for non-negative values (must_be_positive = True)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}
                
                NULL 
            ), 
        '')

    {% elif fundamental_type == 'fundamental_2' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. Rule: Check for valid stock exchanges
                CASE 
                    WHEN UPPER(exchange) NOT IN ('UPCOM', 'HSX', 'HOSE', 'HNX') 
                    THEN 'invalid_exchange' 
                    ELSE NULL 
                END,

                -- 2. Rule: Check listing status (Must be actively listed)
                CASE 
                    WHEN is_listing = FALSE 
                    THEN 'delisted_or_inactive' 
                    ELSE NULL 
                END,

                -- 3. Check for NULLs in mandatory columns
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is mandatory but null' ELSE NULL END,
                    {% endif %}
                {% endfor %}
                
                NULL -- Trick to prevent trailing pipe errors
            ), 
        '')
    {% elif fundamental_type == 'fundamental_quarter' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. AUTO-CHECK NULLS FOR MANDATORY COLUMNS (is_mandatory)
                {% for ind in indicators %}
                    {% if ind.is_mandatory %}
                CASE WHEN {{ ind.alias }} IS NULL THEN '{{ ind.alias }} is mandatory but null' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- 2. AUTO-CHECK FOR NON-NEGATIVE VALUES (must_be_positive)
                {% for ind in indicators %}
                    {% if ind.must_be_positive %}
                CASE WHEN {{ ind.alias }} < 0 THEN '{{ ind.alias }} cannot be negative' ELSE NULL END,
                    {% endif %}
                {% endfor %}

                -- 3. CUSTOM BUSINESS LOGIC CHECKS (If needed)
                -- Example: Market Cap must be greater than 0
                CASE WHEN market_cap = 0 THEN 'Err: Market Cap is zero' ELSE NULL END
            ), 
        '')
    {% endif %}

{% endmacro %}