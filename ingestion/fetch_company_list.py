import pandas as pd
import time
from vnstock import Listing




listing = Listing(source='VCI')
df_listing = listing.symbols_by_industries()




