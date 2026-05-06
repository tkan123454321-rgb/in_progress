select bronze_ingested_time
from "lakehouse_main"."staging"."staging_fundamental_quarter"
where bronze_ingested_time is null
