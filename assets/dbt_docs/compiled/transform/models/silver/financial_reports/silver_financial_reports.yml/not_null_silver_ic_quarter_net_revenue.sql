select net_revenue
from "lakehouse_main"."silver"."silver_ic_quarter"
where net_revenue is null
