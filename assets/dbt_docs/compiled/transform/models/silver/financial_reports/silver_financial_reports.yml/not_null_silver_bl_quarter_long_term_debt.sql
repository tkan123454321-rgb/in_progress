select long_term_debt
from "lakehouse_main"."silver"."silver_bl_quarter"
where long_term_debt is null
