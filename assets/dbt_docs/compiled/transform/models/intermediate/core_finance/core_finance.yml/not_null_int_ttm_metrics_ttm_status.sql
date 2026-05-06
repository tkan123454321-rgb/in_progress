select ttm_status
from "lakehouse_main"."intermediate"."int_ttm_metrics"
where ttm_status is null
