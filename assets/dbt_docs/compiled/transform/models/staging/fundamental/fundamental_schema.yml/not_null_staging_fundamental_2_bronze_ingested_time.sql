select bronze_ingested_time
from "lakehouse_main"."staging"."staging_fundamental_2"
where bronze_ingested_time is null
