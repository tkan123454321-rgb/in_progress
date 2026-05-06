select delta_receivables
from "lakehouse_main"."silver"."silver_cf_quarter"
where delta_receivables is null
