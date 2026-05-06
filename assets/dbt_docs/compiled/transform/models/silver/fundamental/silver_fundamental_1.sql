with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker order by bronze_ingested_time DESC
            ) as rn
        from "lakehouse_main"."staging"."staging_fundamental_1"
    ),

    applied_dq_rules as (
        select
            *,

            NULLIF(
                CONCAT_WS(
                    ' | ',
                    -- 1. Check for NULLs in mandatory columns (is_mandatory = True)
                    case
                        when shares_outstanding is NULL
                        then 'shares_outstanding is mandatory but null'
                        else NULL
                    end,

                    case
                        when market_cap is NULL
                        then 'market_cap is mandatory but null'
                        else NULL
                    end,

                    case
                        when avg_volume_3m is NULL
                        then 'avg_volume_3m is mandatory but null'
                        else NULL
                    end,

                    -- 2. Check for non-negative values (must_be_positive = True)
                    NULL
                ),
                ''
            ) as unqualified_reason

        from deduped_data
        where rn = 1
    )

select
    ticker,
    shares_outstanding,
    floating_shares,
    market_cap,
    avg_volume_3m,
    insider_ownership,
    institution_ownership,
    foreign_ownership,

    case
        when unqualified_reason is NULL then 'qualified' else 'unqualified'
    end as status,
    unqualified_reason,

    CAST(
        from_iso8601_timestamp('2026-05-06T08:01:34.665195+00:00') as TIMESTAMP
        with TIME ZONE
    ) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at,

    'd5a816e0-a4c8-4d5b-bf97-ac0fe62d468a' as silver_invocation_id

from applied_dq_rules
