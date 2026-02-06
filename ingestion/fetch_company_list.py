import pandas as pd
import time
from vnstock import Listing




listing = Listing(source='VCI')

 
df_listing = listing.symbols_by_industries()
hose_list = listing.symbols_by_group('HOSE').astype(str).str.upper().str.strip().to_list() # danh sách cổ phiếu sàn HOSE
hnx_list = listing.symbols_by_group('HNX').astype(str).str.upper().str.strip().to_list() # danh sách cổ phiếu sàn HNX
white_list = set(hose_list + hnx_list) # tập hợp cổ phiếu sàn HOSE và HNX
df_listing = df_listing[df_listing['symbol'].astype(str).str.upper().str.strip().isin(white_list)] # chỉ giữ lại cổ phiếu trong white_list
df_listing = df_listing.drop(['icb_name2','icb_name4','com_type_code','icb_code1','icb_code2','icb_code3','icb_code4'], axis=1)
df_listing = df_listing.rename(columns={
    'symbol': 'Ticker',
    'organ_name': 'Company Name',
    'icb_name3': 'Industry Name',
})

df_listing['Ticker'] = df_listing['Ticker'].astype(str)
df_listing['Company Name'] = df_listing['Company Name'].astype(str)
df_listing['Industry Name'] = df_listing['Industry Name'].astype(str)
df_listing = df_listing.reset_index(drop=True)




