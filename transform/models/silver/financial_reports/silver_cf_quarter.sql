{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns",
        tags=["silver", "cash_flow_indirect_quarter", "quarter"],
        unique_key=["ticker", "year", "quarter"],
        incremental_strategy="merge",
    )
}}

{% set indicators = get_financial_reports_column("cash_flow_indirect") %}
{% set audit_cols = get_audit_columns("silver") %}


-- STEP 1: DEDUPLICATE BRONZE DATA
-- Retrieve the latest record for each ticker, year, quarter, and indicator_id based
-- on ingestion time.
with
    watermark as (
        select
            COALESCE(
                MAX(silver_updated_at),
                CAST('1900-01-01 00:00:00 UTC' as TIMESTAMP with TIME ZONE)
            ) as max_time
        from {{ this }}
    ),

    new_data as (
        select *
        from {{ source("bronze", "financial_reports_quarter") }}
        where
            year >= 2018 and data_type in ('cash_flow_indirect_quarter')
            {% if is_incremental() %}
                and bronze_ingested_time > (select max_time from watermark)
            {% endif %}
    ),

    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker, year, quarter, indicator_id
                order by bronze_ingested_time DESC
            ) as rn
        from new_data
    ),
    -- STEP 2: PIVOT INDICATORS
    -- Transform the data from long format (rows) to wide format (columns).
    pivoted_data as (
        select
            ticker,
            year,
            quarter

            {% for ind in indicators %}
                ,
                MAX(
                    case
                        when indicator_id = {{ ind.id }}
                        then CAST(value as {{ ind.type }})
                    end
                ) as {{ ind.alias }}
            {% endfor %}

        from deduped_data
        where rn = 1
        group by ticker, year, quarter
    ),
    -- STEP 3: APPLY DATA QUALITY RULES
    -- Evaluate data against predefined Data Quality rules to capture the unqualified
    -- reason.
    applied_dq_rules as (
        select
            *, {{ check_financial_reports("cash_flow_indirect") }} as unqualified_reason
        from pivoted_data
    )

-- STEP 4: FINAL SELECTION & FORMATTING
-- Handle null values, append system audit columns, and determine the final DQ status.
select
    ticker,
    year,
    quarter,

    -- Replace NULL values with 0 for all financial indicators
    {% for ind in indicators %}
        COALESCE({{ ind.alias }}, 0) as {{ ind.alias }},
    {% endfor %}

    {% for col in audit_cols %} {{ col.expr }} as {{ col.alias }}, {% endfor %}

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason

from applied_dq_rules
