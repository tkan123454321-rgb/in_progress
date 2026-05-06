select retained_earnings
from "lakehouse_main"."silver"."silver_bl_quarter"
where retained_earnings is null
