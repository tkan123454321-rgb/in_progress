{{ config(
    unique_key='ticker'
) }}

{% set audit_cols = get_audit_columns('staging') %}

SELECT
    -- 1. Làm sạch dữ liệu nghiệp vụ
    TRIM(UPPER(symbol)) AS ticker,
    NULLIF(TRIM(organ_name), '') AS company_name,
    NULLIF(TRIM(icb_name2), '') AS industry_group,
    NULLIF(TRIM(icb_name4), '') AS sector_detail,
    com_type_code,
    {% for col in audit_cols %}
        {{ col.expr }} AS {{ col.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

FROM {{ source('bronze', 'original_ticker_list') }}