select gold_updated_at
from "lakehouse_main"."gold"."gold_value_and_momentum_z_recent"
where gold_updated_at is null
