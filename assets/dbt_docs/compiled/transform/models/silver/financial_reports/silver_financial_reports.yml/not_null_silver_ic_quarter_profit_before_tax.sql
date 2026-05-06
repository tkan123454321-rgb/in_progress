select profit_before_tax
from "lakehouse_main"."silver"."silver_ic_quarter"
where profit_before_tax is null
