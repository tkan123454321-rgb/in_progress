select ticker
from "lakehouse_main"."silver"."silver_fundamental_quarter"
where ticker is null
