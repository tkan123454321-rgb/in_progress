select selling_expense
from "lakehouse_main"."silver"."silver_ic_quarter"
where selling_expense is null
