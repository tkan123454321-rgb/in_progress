select admin_expense
from "lakehouse_main"."silver"."silver_ic_quarter"
where admin_expense is null
