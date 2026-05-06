select short_term_receivables
from "lakehouse_main"."silver"."silver_bl_quarter"
where short_term_receivables is null
