{{ config(materialized="ephemeral") }}

select *
from {{ ref("gold_dim_company") }}
where status = 'qualified'
