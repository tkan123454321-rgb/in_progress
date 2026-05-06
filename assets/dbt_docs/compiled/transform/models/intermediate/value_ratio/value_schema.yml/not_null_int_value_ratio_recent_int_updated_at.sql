select int_updated_at
from "lakehouse_main"."intermediate"."int_value_ratio_recent"
where int_updated_at is null
