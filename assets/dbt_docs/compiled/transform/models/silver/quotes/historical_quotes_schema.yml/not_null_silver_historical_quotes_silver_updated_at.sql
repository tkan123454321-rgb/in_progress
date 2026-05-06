select silver_updated_at
from "lakehouse_main"."silver"."silver_historical_quotes"
where silver_updated_at is null
