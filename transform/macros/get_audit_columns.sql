{% macro generate_audit_columns(layer_name) %}

    {% if layer_name == 'staging' %}
        -- 1. Lớp Staging: Đổi tên cột gốc và đẻ ra 2 mốc đánh dấu mới
        bronze_ingested_time AT TIME ZONE 'Asia/Ho_Chi_Minh' AS bronze_ingested_time,
        CAST(from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS staged_at,
        '{{ invocation_id }}' AS staging_invocation_id

    {% elif layer_name == 'silver' %}
        -- 2. Lớp Silver: Chỉ GỌI TÊN 3 cột cũ để kéo lên, và đẻ thêm 2 cột mới
        bronze_ingested_time,
        staged_at,
        staging_invocation_id,
        CAST(from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS silver_updated_at,
        '{{ invocation_id }}' AS silver_invocation_id

    {% elif layer_name == 'gold' %}
        CAST(from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS gold_updated_at,
        '{{ invocation_id }}' AS gold_invocation_id

    {% else %}
        {{ exceptions.raise_compiler_error("Tên layer_name không hợp lệ! Hãy chọn 'staging', 'silver', hoặc 'gold'.") }}
    {% endif %}

{% endmacro %}