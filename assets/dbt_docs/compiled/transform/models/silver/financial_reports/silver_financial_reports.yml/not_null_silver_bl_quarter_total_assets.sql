select total_assets
from "lakehouse_main"."silver"."silver_bl_quarter"
where total_assets is null
