select total_volume
from "lakehouse_main"."silver"."silver_historical_quotes"
where total_volume is null
