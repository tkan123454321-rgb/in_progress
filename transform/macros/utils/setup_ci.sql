{% macro setup_ci() %}

    {% set create_schema_sql %}
        CREATE SCHEMA IF NOT EXISTS lakehouse_main.bronze;
    {% endset %}

    {% do run_query(create_schema_sql) %}

    {{ log("created bronze schema for CI environment!", info=True) }}

{% endmacro %}
