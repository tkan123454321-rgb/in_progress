select silver_updated_at
from "lakehouse_main"."silver"."silver_fundamental_2"
where silver_updated_at is null
