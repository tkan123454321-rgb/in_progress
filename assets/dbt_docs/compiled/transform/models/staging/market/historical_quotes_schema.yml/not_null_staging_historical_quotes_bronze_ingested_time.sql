
    
    



select bronze_ingested_time
from "lakehouse_main"."staging"."staging_historical_quotes"
where bronze_ingested_time is null


