select ticker
from "lakehouse_main"."intermediate"."int_ttm_metrics"
where ticker is null
