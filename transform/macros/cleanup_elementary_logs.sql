{% macro cleanup_elementary_logs(days=30) %}

    {% set target_database = 'lakehouse_main' %}
    {% set target_schema = 'elementary' %}
    
    {% set tables_config = {
        'dbt_run_results': {'col': 'created_at'}
    } %}

    {% if execute %}
        {% do log('Bắt đầu dọn dẹp log elementary cũ hơn ' ~ days, info=True) %}

        {% for table_name, config in tables_config.items() %}
            {# check if the table exists before trying to clean it up #}
            {% set relation_exists = adapter.get_relation(
                database=target_database,
                schema=target_schema,
                identifier=table_name
            ) %}

            {% if relation_exists %}
                {{ log(relation_exists.type ~ " exists: " ~ relation_exists.identifier, info=True) }}
        
                {% set cleanup_query %}
                    DELETE FROM {{ relation_exists }}
                    WHERE {{ config['col'] }} < current_date - interval '{{ days }}' day
                {% endset %}
                {% do run_query(cleanup_query) %}
                {% do log('Dọn xong: ' ~ relation_exists, info=True) %}
                
            {% else %}
                {% do log('Bỏ qua: ' ~ relation_exists ~ ' không tồn tại', info=True) %}
            {% endif %}
            
        {% endfor %}

    {% endif %}

{% endmacro %}