select company_name
from "lakehouse_main"."silver"."silver_dim_company"
where company_name is null
