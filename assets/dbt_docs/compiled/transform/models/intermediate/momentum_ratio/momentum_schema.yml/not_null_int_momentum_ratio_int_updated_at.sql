select int_updated_at
from "lakehouse_main"."intermediate"."int_momentum_ratio"
where int_updated_at is null
