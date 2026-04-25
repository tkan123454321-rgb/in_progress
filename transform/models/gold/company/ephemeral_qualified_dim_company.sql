{{ config(
    materialized='ephemeral'
) }}

SELECT * FROM {{ ref('gold_dim_company') }}
WHERE status = 'qualified'