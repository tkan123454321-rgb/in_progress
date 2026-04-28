{% test null_rate(model, column_name) %}

{{ config(fail_calc = 'max(failure_bps)') }}

WITH validation AS (
    SELECT
        COUNT(CASE WHEN {{ column_name }} IS NULL THEN 1 END) AS null_count,
        COUNT(*) AS total_count
    FROM {{ model }}
)

SELECT
    -- Multiply by 10,000 first to get basis points (bps), then divide. 
    -- Cast to INTEGER to align with dbt's threshold evaluation logic.
    CAST((null_count * 10000) / NULLIF(total_count, 0) AS INTEGER) AS failure_bps
FROM validation

{% endtest %}