select income_tax_expense
from "lakehouse_main"."silver"."silver_ic_quarter"
where income_tax_expense is null
