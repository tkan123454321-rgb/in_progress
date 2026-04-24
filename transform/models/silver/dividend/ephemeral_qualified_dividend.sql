{{ config(
    materialized='ephemeral',
    description="This ephemeral model serves exclusively as a clean data feed for the 'dividend_snapshot'. It filters out any unqualified records from the Silver layer before taking the snapshot."
) }}

SELECT * FROM {{ ref('silver_dividend') }}
WHERE status = 'qualified'