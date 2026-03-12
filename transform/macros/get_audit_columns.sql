{% macro generate_audit_columns(layer_name) %}

    {% if layer_name == 'staging' %}
        -- Lớp Staging đẻ ra 2 cột mới
        TRY_CAST(inserted_time AS TIMESTAMP) AT TIME ZONE 'Asia/Ho_Chi_Minh' AS inserted_bronze_time,
        '{{ invocation_id }}' AS staging_invocation_id,
        from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AT TIME ZONE 'Asia/Ho_Chi_Minh' AS staged_at,
        

    {% elif layer_name == 'silver' %}
        -- Lớp Silver: Kế thừa của Staging, và đẻ thêm 2 cột mới của Silver
        staging_invocation_id,
        staged_at,
        '{{ invocation_id }}' AS silver_invocation_id,
        from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AT TIME ZONE 'Asia/Ho_Chi_Minh' AS silver_updated_at
    {% elif layer_name == 'gold' %}
        -- Lớp Gold: Kế thừa của Silver, và đẻ thêm 2 cột mới của Gold
        '{{ invocation_id }}' AS gold_invocation_id,
        from_iso8601_timestamp('{{ run_started_at.isoformat() }}') AT TIME ZONE 'Asia/Ho_Chi_Minh' AS gold_updated_at
    {% else %}

        {{ exceptions.raise_compiler_error("Invalid layer_name passed to generate_audit_columns(). Choose 'staging', 'silver', or 'gold'.") }}
    {% endif %}

{% endmacro %}