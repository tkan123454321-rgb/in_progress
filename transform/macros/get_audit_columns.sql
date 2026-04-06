{% macro get_audit_columns(layer_name) %}

    {% if layer_name == 'staging' %}
        {{ return([
            {'alias': 'bronze_ingested_time', 'expr': "bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh'", 'needs_agg': True, 'is_from_staging': False},
            {'alias': 'staged_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'", 'needs_agg': False, 'is_from_staging': False},
            {'alias': 'staging_invocation_id', 'expr': "'" ~ invocation_id ~ "'", 'needs_agg': False, 'is_from_staging': False}
        ]) }}

    {% elif layer_name == 'silver' %}
        {{ return([
            {'alias': 'bronze_ingested_time', 'expr': 'bronze_ingested_time', 'needs_agg': True, 'is_from_staging': False},
            
            {'alias': 'staged_at', 'expr': 'staged_at', 'needs_agg': True, 'is_from_staging': True},
            {'alias': 'staging_invocation_id', 'expr': 'staging_invocation_id', 'needs_agg': True, 'is_from_staging': True},
            
            {'alias': 'silver_updated_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'", 'needs_agg': False, 'is_from_staging': False},
            {'alias': 'silver_invocation_id', 'expr': "'" ~ invocation_id ~ "'", 'needs_agg': False, 'is_from_staging': False}
        ]) }}

    {% elif layer_name == 'gold' %}
        {{ return([
            {'alias': 'gold_updated_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'", 'needs_agg': False, 'is_from_staging': False},
            {'alias': 'gold_invocation_id', 'expr': "'" ~ invocation_id ~ "'", 'needs_agg': False, 'is_from_staging': False}
        ]) }}
    {% elif layer_name == 'intermediate' %}
        {{ return([
            {'alias': 'int_updated_at', 'expr': "CAST(from_iso8601_timestamp('" ~ run_started_at.isoformat() ~ "') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh'", 'needs_agg': False, 'is_from_staging': False},
            {'alias': 'int_invocation_id', 'expr': "'" ~ invocation_id ~ "'", 'needs_agg': False, 'is_from_staging': False}
        ]) }}

    {% else %}
        {{ exceptions.raise_compiler_error("Tên layer_name không hợp lệ! Hãy chọn 'staging', 'silver', hoặc 'gold'.") }}
    {% endif %}

{% endmacro %}