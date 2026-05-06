select shares_outstanding
from "lakehouse_main"."silver"."silver_fundamental_quarter"
where shares_outstanding is null
