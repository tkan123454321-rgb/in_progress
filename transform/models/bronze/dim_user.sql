SELECT *
FROM {{ ref('test_raw_users') }}  -- Dòng quan trọng nhất đây!
WHERE status = 'active'