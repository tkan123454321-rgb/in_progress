{{ config(
    materialized='ephemeral',
    description="Filters only 'qualified' company records from the gold layer to feed into the SCD Type 2 snapshot."
) }}

SELECT * FROM {{ ref('gold_dim_company') }}
WHERE status = 'qualified'