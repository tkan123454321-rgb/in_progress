select ticker
from "lakehouse_main"."silver"."silver_historical_quotes"
where ticker is null
