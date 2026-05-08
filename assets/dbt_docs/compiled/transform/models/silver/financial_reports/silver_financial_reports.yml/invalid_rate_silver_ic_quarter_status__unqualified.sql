

    

    with
        validation as (
            select
                COUNT(
                    case when status = 'unqualified' then 1 end
                ) as invalid_count,
                COUNT(*) as total_count
            from "lakehouse_main"."silver"."silver_ic_quarter"
        )

    select
        -- Multiply by 10,000 first to get basis points (bps), then divide.
        -- Cast to INTEGER to align with dbt's threshold evaluation logic.
        CAST((invalid_count * 10000) / NULLIF(total_count, 0) as INTEGER) as failure_bps
    from validation

