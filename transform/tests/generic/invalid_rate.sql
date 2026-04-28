{% test invalid_rate(model, column_name, invalid_value) %}

{{ config(fail_calc = 'max(failure_bps)') }}

WITH validation AS (
    SELECT
        COUNT(CASE WHEN {{ column_name }} = '{{ invalid_value }}' THEN 1 END) AS invalid_count,
        COUNT(*) AS total_count
    FROM {{ model }}
)

SELECT
    -- Multiply by 10,000 first to get basis points (bps), then divide. 
    -- Cast to INTEGER to align with dbt's threshold evaluation logic.
    CAST((invalid_count * 10000) / NULLIF(total_count, 0) AS INTEGER) AS failure_bps
FROM validation

{% endtest %}