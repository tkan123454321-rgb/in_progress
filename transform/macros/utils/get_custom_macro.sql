{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# Nếu không đặt tên riêng thì dùng mặc định #}
        {{ default_schema }}
    {%- else -%}
        {# Nếu đã đặt tên riêng (ví dụ: silver) thì dùng đúng tên đó, cắt bỏ cái đuôi rườm rà #}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
