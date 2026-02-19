
{% macro test_check_relation(table_name) %}
    {# Macro này để kiểm tra xem dbt có nhìn thấy bảng trên Trino không #}
    {% if execute %}
        {{ log("============== 🔍 DEBUG: " ~ table_name ~ " ==============", info=True) }}
        
        {% set relation = adapter.get_relation(
            database='lakehouse',
            schema='elementary', 
            identifier=table_name
        ) %}

        {% if relation is none %}
            {{ log("❌ Kết quả: NONE (Bảng chưa tồn tại hoặc sai tên/schema)", info=True) }}
        {% else %}
            {{ log("✅ Kết quả: OBJECT (Bảng tồn tại)", info=True) }}
            {{ log("   -> Type: " ~ relation.type, info=True) }}
        {% endif %}
        
        {{ log("======================================================", info=True) }}
    {% endif %}
{% endmacro %}