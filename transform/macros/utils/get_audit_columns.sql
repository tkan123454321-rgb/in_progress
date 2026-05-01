{% macro get_audit_columns(layer_name) %}

    {% if layer_name == 'staging' %}
        {{ return([
            {'alias': 'bronze_ingested_time', 'expr': "bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh'"},
            {'alias': 'staged_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'"},
            {'alias': 'staging_invocation_id', 'expr': "'" ~ var("airflow_run_id", invocation_id) ~ "'", 'needs_agg': False, 'is_from_staging': False}
        ]) }}

    {% elif layer_name == 'silver' %}
        {{ return([
            {'alias': 'silver_updated_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'"},
            {'alias': 'silver_invocation_id', 'expr': "'" ~ var("airflow_run_id", invocation_id) ~ "'", 'needs_agg': False, 'is_from_staging': False}
        ]) }}

    {% elif layer_name == 'gold' %}
        {{ return([
            {'alias': 'gold_updated_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'"},
            {'alias': 'gold_invocation_id', 'expr': "'" ~ var("airflow_run_id", invocation_id) ~ "'"}
        ]) }}
        
    {% elif layer_name == 'intermediate' %}
        {{ return([
            {'alias': 'int_updated_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'"},
            {'alias': 'int_invocation_id', 'expr': "'" ~ var("airflow_run_id", invocation_id) ~ "'"}
        ]) }}
        
    {% else %}
        {{ exceptions.raise_compiler_error("Tên layer_name không hợp lệ! Hãy chọn 'staging', 'silver', hoặc 'gold'.") }}
    {% endif %}

{% endmacro %}