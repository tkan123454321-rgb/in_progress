


with
    deduped_data as (
        select
            *,
            ROW_NUMBER() over (
                partition by ticker
                order by company_name ASC, industry_group ASC, sector_detail ASC
            ) as rn
        from "lakehouse_main"."seeds"."bronze_dim_company"
        where
            ticker is not NULL
            and regexp_like(ticker, '^[A-Z0-9]{3}$')
            and company_type is not NULL
    )
select
    ticker,
    COALESCE(company_name, 'Unknown Company') as company_name,
    COALESCE(industry_group, 'Unclassified') as industry_group,
    COALESCE(sector_detail, 'Unclassified') as sector_detail,
    company_type
    ,CAST(from_iso8601_timestamp('2026-05-06T08:58:52.723406+00:00') AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Ho_Chi_Minh' as silver_updated_at ,'4ff423e7-7675-4eec-a090-58bdf9560b12' as silver_invocation_id 
from deduped_data
where rn = 1