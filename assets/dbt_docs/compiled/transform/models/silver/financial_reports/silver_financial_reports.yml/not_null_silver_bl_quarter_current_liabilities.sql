select current_liabilities
from "lakehouse_main"."silver"."silver_bl_quarter"
where current_liabilities is null
