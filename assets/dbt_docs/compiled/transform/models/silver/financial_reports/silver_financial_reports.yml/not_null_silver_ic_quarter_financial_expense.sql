select financial_expense
from "lakehouse_main"."silver"."silver_ic_quarter"
where financial_expense is null
