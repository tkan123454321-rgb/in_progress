select int_updated_at
from "lakehouse_main"."intermediate"."int_ttm_metrics"
where int_updated_at is null
