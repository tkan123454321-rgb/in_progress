select income_taxes_payable
from "lakehouse_main"."silver"."silver_bl_quarter"
where income_taxes_payable is null
