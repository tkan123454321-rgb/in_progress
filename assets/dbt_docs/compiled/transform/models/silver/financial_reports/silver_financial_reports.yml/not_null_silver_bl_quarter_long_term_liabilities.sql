select long_term_liabilities
from "lakehouse_main"."silver"."silver_bl_quarter"
where long_term_liabilities is null
