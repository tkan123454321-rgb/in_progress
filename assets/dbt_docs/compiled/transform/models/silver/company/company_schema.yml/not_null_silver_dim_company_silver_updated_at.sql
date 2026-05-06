select silver_updated_at
from "lakehouse_main"."silver"."silver_dim_company"
where silver_updated_at is null
