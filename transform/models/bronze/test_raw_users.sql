

WITH raw_data AS (
    SELECT 1 AS id, 'Nguyen Van A' AS name, '2024-01-01' AS created_at, 'active' AS status
    UNION ALL
    SELECT 2 AS id, 'Tran Thi B' AS name, '2024-01-02' AS created_at, 'inactive' AS status
    UNION ALL
    SELECT 3 AS id, 'Le Van C' AS name, '2024-01-03' AS created_at, 'active' AS status
)

SELECT * FROM raw_data