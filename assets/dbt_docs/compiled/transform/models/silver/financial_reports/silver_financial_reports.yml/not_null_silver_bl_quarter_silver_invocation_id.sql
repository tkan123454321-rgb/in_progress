select silver_invocation_id
from "lakehouse_main"."silver"."silver_bl_quarter"
where silver_invocation_id is null
