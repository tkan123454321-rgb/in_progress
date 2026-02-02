import pandas as pd
import time
from vnstock import Listing




def update_companies_list(engine):
    listing = Listing(source='VCI')

    # cập nhật bảng company_list
    df_listing = listing.symbols_by_industries()
    blacklist =['CK','NH','BH'] # loại bỏ cổ phiếu chứng khoán, ngân hàng, bảo hiểm
    df_listing = df_listing[~df_listing['com_type_code'].isin(blacklist)] # loại bỏ cổ phiếu trong blacklist
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

    df_listing.to_sql('temp_table', engine, schema = 'raw', if_exists='replace', index=False)  
    if inspector.has_table('companies_list', schema='analysis_data'):
        query = """
        INSERT INTO analysis_data.companies_list ("Ticker", "Company Name", "Industry Name")
        SELECT "Ticker", "Company Name", "Industry Name"
        FROM raw.temp_table
        ON CONFLICT ("Ticker")
        DO UPDATE SET
            "Company Name" = EXCLUDED."Company Name",
            "Industry Name" = EXCLUDED."Industry Name";
        """ # thêm dữ liệu vào bảng companies_list, nếu đã tồn tại thì cập nhật lại thông tin

        drop_temp_table = "DROP TABLE IF EXISTS raw.temp_table;" # xóa bảng tạm temp_table

        with engine.connect() as conn:
            conn.execute(text(query)) # Execute DML to insert/update data
            conn.execute(text(drop_temp_table)) # Drop temp_table
            conn.commit()





