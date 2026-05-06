select current_assets
from "lakehouse_main"."silver"."silver_bl_quarter"
where current_assets is null
