select int_updated_at
from "lakehouse_main"."intermediate"."int_qmj_growth"
where int_updated_at is null
