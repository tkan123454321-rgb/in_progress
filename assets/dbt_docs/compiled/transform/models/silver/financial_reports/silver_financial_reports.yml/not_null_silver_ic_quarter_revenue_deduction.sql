select revenue_deduction
from "lakehouse_main"."silver"."silver_ic_quarter"
where revenue_deduction is null
