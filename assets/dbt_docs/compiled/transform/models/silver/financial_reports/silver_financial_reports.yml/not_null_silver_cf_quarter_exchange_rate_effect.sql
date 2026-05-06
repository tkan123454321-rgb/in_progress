select exchange_rate_effect
from "lakehouse_main"."silver"."silver_cf_quarter"
where exchange_rate_effect is null
