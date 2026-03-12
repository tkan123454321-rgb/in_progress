{% test invalid_rate(model, column_name, invalid_value) %}

{{ config(fail_calc = 'max(failure_bps)') }}

WITH validation AS (
    SELECT
        COUNT(CASE WHEN {{ column_name }} = '{{ invalid_value }}' THEN 1 END) AS invalid_count,
        COUNT(*) AS total_count
    FROM {{ model }}
)

SELECT
    -- Nhân 10000 trước rồi mới chia. Chỉ cần 1 hàm CAST duy nhất cho thằng dbt nó vừa lòng!
    CAST((invalid_count * 10000) / NULLIF(total_count, 0) AS INTEGER) AS failure_bps
FROM validation

{% endtest %}