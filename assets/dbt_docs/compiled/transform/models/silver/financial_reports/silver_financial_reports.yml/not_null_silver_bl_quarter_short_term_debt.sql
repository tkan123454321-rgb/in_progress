select short_term_debt
from "lakehouse_main"."silver"."silver_bl_quarter"
where short_term_debt is null
