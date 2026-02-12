from zoneinfo import ZoneInfo
import pandas as pd
import time
from vnstock import Listing
import polars as pl
from datetime import datetime

current_time = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
current_time = current_time.replace(microsecond=0, tzinfo=None)

def _fetch_company_list():
    listing = Listing(source='VCI')
    df = listing.symbols_by_industries()
    df = pl.from_pandas(df)
    df = df.with_columns(ingestion_time = pl.lit(current_time))
    df_arrow = df.to_arrow()
    return df_arrow





