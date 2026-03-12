{% test null_rate(model, column_name) %}

{{ config(fail_calc = 'max(failure_bps)') }}

WITH validation AS (
    SELECT
        COUNT(CASE WHEN {{ column_name }} IS NULL THEN 1 END) AS null_count,
        COUNT(*) AS total_count
    FROM {{ model }}
)

SELECT
    -- Nhân 10000 trước rồi mới chia. Ép về INTEGER cho dbt nó vui!
    CAST((null_count * 10000) / NULLIF(total_count, 0) AS INTEGER) AS failure_bps
FROM validation

{% endtest %}