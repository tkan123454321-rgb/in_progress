{% macro get_max_timestamp(
    source_name, table_name, column_name="bronze_ingested_time"
) %}
    {#

        Args:
            source_name (str): Tên source (vd: 'bronze')
            table_name (str): Tên bảng (vd: 'fundamental')
            column_name (str): Cột cần lấy max time (mặc định: 'bronze_ingested_time')

        Returns (str)
    #}
    {% set query %}
        SELECT MAX({{ column_name }}) FROM {{ source(source_name, table_name) }}
    {% endset %}

    {% set result = dbt_utils.get_single_value(query) %}

    {% if not execute %} {{ return("1900-01-01") }}
    {% elif result %} {{ return(result) }}
    {% else %}
        {{
            exceptions.raise_compiler_error(
                "ERROR: The source table "
                ~ source_name
                ~ "."
                ~ table_name
                ~ " is empty or the column '"
                ~ column_name
                ~ "' is empty."
            )
        }}
    {% endif %}
{% endmacro %}
