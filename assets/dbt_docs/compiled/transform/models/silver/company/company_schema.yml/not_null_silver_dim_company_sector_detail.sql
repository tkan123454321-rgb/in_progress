select sector_detail
from "lakehouse_main"."silver"."silver_dim_company"
where sector_detail is null
