select borrowings_received
from "lakehouse_main"."silver"."silver_cf_quarter"
where borrowings_received is null
