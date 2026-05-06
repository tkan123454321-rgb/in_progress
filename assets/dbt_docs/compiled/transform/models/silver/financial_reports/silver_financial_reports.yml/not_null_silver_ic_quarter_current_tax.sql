select current_tax
from "lakehouse_main"."silver"."silver_ic_quarter"
where current_tax is null
