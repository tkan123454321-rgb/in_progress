

    

    with
        validation as (
            select
                COUNT(case when floating_shares is NULL then 1 end) as null_count,
                COUNT(*) as total_count
            from "lakehouse_main"."silver"."silver_fundamental_1"
        )

    select
        -- Multiply by 10,000 first to get basis points (bps), then divide.
        -- Cast to INTEGER to align with dbt's threshold evaluation logic.
        CAST((null_count * 10000) / NULLIF(total_count, 0) as INTEGER) as failure_bps
    from validation

