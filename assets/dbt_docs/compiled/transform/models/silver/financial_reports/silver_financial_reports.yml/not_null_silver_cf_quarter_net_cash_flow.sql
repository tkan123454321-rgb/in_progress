select net_cash_flow
from "lakehouse_main"."silver"."silver_cf_quarter"
where net_cash_flow is null
