import requests
import pandas as pd
import ast
import os
import time
import numpy as np
import traceback
from IPython.display import display
from vnstock import *
from datetime import datetime



# hàm lấy bảng lưu chuyển tiền tệ        
def cash_flow_statement(stock):
    try: 
        # lấy API ẩn từ trang web fireant.vn
        year_today = time.localtime().tm_year
        year_limit = 2018
        limit = year_today - year_limit 
        auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
        url = f'https://restv2.fireant.vn/symbols/{stock}/full-financial-reports?type=4&year={year_today}&quarter=0&limit={limit}'
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


        params = {
            'type': '4',
            'year': str(year_today),
            'quarter': '0',
            'limit': str(limit)
        }

        headers = {
            'User-Agent': user_agent,
            'Authorization': auth_token
        }

        response = requests.get(url, headers=headers, params=params)
        response = response.json()  # đổi dữ liệu về dạng json để python đọc được (dạng dict hoặc list)

        # chuyển dữ liệu về dạng dataframe
        df_cf = pd.DataFrame(response)

        
        # hàm kiểm tra nếu giá trị là str thì chuyển về dạng list hoặc dict
        def convert_if_string(x):
            if isinstance(x,str):
                return ast.literal_eval(x)
            else:
                return x
        # Áp dụng hàm cho từng giá trị trong cột 'values'
        df_cf['values'] = df_cf['values'].apply(convert_if_string)

        # nổ cột 'values' thành nhiều hàng
        df_cf = df_cf.explode('values').reset_index(drop=True)
        # chuyển cột 'values' từ danh sách thành nhiều cột
        df_normalized = pd.json_normalize(df_cf['values'])
        #nối cột df_cf ban đầu với df_normalized
        df_cf = pd.concat([df_cf.drop(columns=[
            'values','id','parentID','expanded','level','field']), df_normalized], axis=1)
        # giữ lại các cột cần thiết
        df_cf = df_cf[['name','year','value']]
        df_cf = df_cf.pivot_table(index='name', columns='year', values='value',dropna = False).reset_index()

        # đổi tên 1 số hàng
        df_cf_doiten = {' - Tăng, giảm hàng tồn kho': '- Tăng, giảm hàng tồn kho', ' - Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)': '- Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)'}
        df_cf['name'] = df_cf['name'].replace(df_cf_doiten)
        #sắp xếp các dòng theo thứ tự ban đầu
        cf_thutu = '''
        I. Lưu chuyển tiền từ hoạt động kinh doanh	
        1. Lợi nhuận trước thuế	
        2. Điều chỉnh cho các khoản	
        - Khấu hao TSCĐ	
        - Các khoản dự phòng	
        - Lợi nhuận thuần từ đầu tư vào công ty liên kết	
        - Xóa sổ tài sản cố định (thuần)	
        - Lãi, lỗ chênh lệch tỷ giá hối đoái chưa thực hiện	
        - Lãi, lỗ từ thanh lý TSCĐ	
        - Lãi, lỗ từ hoạt động đầu tư	
        - Lãi tiền gửi	
        - Thu nhập lãi	
        - Chi phí lãi vay	
        - Các khoản chi trực tiếp từ lợi nhuận	
        3. Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn lưu động	
        - Tăng, giảm các khoản phải thu
        - Tăng, giảm hàng tồn kho
        - Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)
        - Tăng giảm chi phí trả trước	
        - Tăng giảm tài sản ngắn hạn khác	
        - Tiền lãi vay phải trả	
        - Thuế thu nhập doanh nghiệp đã nộp	
        - Tiền thu khác từ hoạt động kinh doanh	
        - Tiền chi khác từ hoạt động kinh doanh	
        Lưu chuyển tiền thuần từ hoạt động kinh doanh	
        II. Lưu chuyển tiền từ hoạt động đầu tư	
        1. Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác	
        2. Tiền thu từ thanh lý, nhượng bán TSCĐ và các tài sản dài hạn khác	
        3. Tiền chi cho vay, mua các công cụ nợ của đơn vị khác	
        4. Tiền thu hồi cho vay, bán lại các công cụ nợ của các đơn vị khác	
        5. Đầu tư góp vốn vào công ty liên doanh liên kết	
        6. Chi đầu tư ngắn hạn	
        7. Tiền chi đầu tư góp vốn vào đơn vị khác	
        8. Tiền thu hồi đầu tư góp vốn vào đơn vị khác	
        9. Lãi tiền gửi đã thu	
        10. Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia	
        11. Tiền chi mua lại phần vốn góp của các cổ đông thiểu số	
        Lưu chuyển tiền thuần từ hoạt động đầu tư	
        III. Lưu chuyển tiền từ hoạt động tài chính	
        1. Tiền thu từ phát hành cổ phiếu, nhận vốn góp của chủ sở hữu	
        2. Tiền chi trả vốn góp cho các chủ sở hữu, mua lại cổ phiếu của doanh nghiệp đã phát hành	
        3. Tiền vay ngắn hạn, dài hạn nhận được	
        4. Tiền chi trả nợ gốc vay	
        5. Tiền chi trả nợ thuê tài chính	
        6. Tiền chi khác từ hoạt động tài chính	
        7. Tiền chi trả từ cổ phần hóa	
        8. Cổ tức, lợi nhuận đã trả cho chủ sở hữu	
        9. Vốn góp của các cổ đông thiểu số vào các công ty con	
        10. Chi tiêu quỹ phúc lợi xã hội	
        Lưu chuyển tiền thuần từ hoạt động tài chính	
        Lưu chuyển tiền thuần trong kỳ	
        Tiền và tương đương tiền đầu kỳ	
        Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ	
        Tiền và tương đương tiền cuối kỳ
        '''
        cf_thutu = cf_thutu.strip().splitlines()
        cf_thutu_processed = [item.strip() for item in cf_thutu ]

        #sắp xếp lại index cho đúng
        df_cf = df_cf.set_index('name').reindex(cf_thutu_processed).reset_index()
        df_cf = df_cf.rename(columns={'name':f"{stock}, Đơn vị: đồng"})
        return df_cf
    except Exception as e:
        print(f"Lỗi khi lấy báo cáo lưu chuyển tiền tệ cho {stock}: {e}")
        try:
            auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
            url = f'https://restv2.fireant.vn/symbols/{stock}/full-financial-reports?type=3&year={year_today}&quarter=0&limit={limit}'
            user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


            params = {
                    'type': '4',
                    'year': str(year_today),
                    'quarter': '0',
                    'limit': str(limit)
            }

            headers = {
                    'User-Agent': user_agent,
                    'Authorization': auth_token
            }

            response = requests.get(url, headers=headers, params=params)
            response = response.json()  # đổi dữ liệu về dạng json để python đọc được (dạng dict hoặc list)

                # chuyển dữ liệu về dạng dataframe
            data = pd.DataFrame(response)
            def convert_if_string(x):
                if isinstance(x,str):
                    return ast.literal_eval(x)
                else:
                    return x
            df_cf = data
                # Áp dụng hàm cho từng giá trị trong cột 'values'
            df_cf['values'] = df_cf['values'].apply(convert_if_string)
                # nổ cột 'values' thành nhiều hàng
            df_cf = df_cf.explode('values').reset_index(drop=True)
                # chuyển cột 'values' từ danh sách thành nhiều cột
            df_normalized = pd.json_normalize(df_cf['values'])
                #nối cột df_cf ban đầu với df_normalized
            df_cf = pd.concat([df_cf.drop(columns=[
                'values','id','parentID','expanded','level','field']), df_normalized], axis=1)
            # giữ lại các cột cần thiết
            df_cf = df_cf[['name','year','value']]
            df_cf = df_cf.pivot_table(index='name', columns='year', values='value',dropna = False).reset_index()
            
            # strip hết khoảng trắng thừa trong cột name
            df_cf['name'] = df_cf['name'].str.strip()
            # sắp xếp thứ tự đúng
            df_cf_sort = '''
            I. Lưu chuyển tiền từ hoạt động kinh doanh	
            1. Tiền thu từ bán hàng, cung cấp dịch vụ và doanh thu khác	
            2. Tiền chi trả cho người cung cấp hàng hóa và dịch vụ	
            3. Tiền chi trả cho người lao động	
            4. Tiền chi trả lãi vay	
            5. Tiền chi nộp thuế thu nhập doanh nghiệp	
            6. Tiền chi nộp thuế giá trị gia tăng	
            7. Tiền thu khác từ hoạt động kinh doanh	
            8. Tiền chi khác cho hoạt động kinh doanh	
            Lưu chuyển tiền thuần từ hoạt động kinh doanh	
            II. Lưu chuyển tiền từ hoạt động đầu tư	
            1. Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác	
            2. Tiền thu từ thanh lý, nhượng bán TSCĐ và các tài sản dài hạn khác	
            3. Tiền chi cho vay, mua các công cụ nợ của đơn vị khác	
            4. Tiền thu hồi cho vay, bán lại các công cụ nợ của các đơn vị khác	
            5. Tiền chi đầu tư góp vốn vào đơn vị khác		
            6. Tiền thu hồi đầu tư góp vốn vào đơn vị khác	
            7. Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia	
            Lưu chuyển tiền thuần từ hoạt động đầu tư	
            III. Lưu chuyển tiền từ hoạt động tài chính	
            1. Tiền thu từ phát hành cổ phiếu, nhận vốn góp của chủ sở hữu	
            2. Tiền chi trả vốn góp cho các chủ sở hữu, mua lại cổ phiếu của doanh nghiệp đã phát hành	
            3. Tiền vay ngắn hạn, dài hạn nhận được	
            4. Tiền chi trả nợ gốc vay	
            5. Tiền chi để mua sắm, xây dựng TSCĐ, BĐS đầu tư	
            6. Tiền chi trả nợ thuê tài chính	
            7. Cổ tức, lợi nhuận đã trả cho chủ sở hữu	
            8. Chi từ các quỹ của doanh nghiệp	
            Lưu chuyển tiền thuần từ hoạt động tài chính	
            Lưu chuyển tiền thuần trong kỳ	
            Tiền và tương đương tiền đầu kỳ	
            Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ	
            Tiền và tương đương tiền cuối kỳ
            '''
            df_cf_sort = df_cf_sort.strip().splitlines()
            df_cf_sort_processed = [item.strip() for item in df_cf_sort ]
            df_cf = df_cf.set_index('name').reindex(df_cf_sort_processed).reset_index()
            print("Lấy báo cáo lưu chuyển tiền tệ lần 2 thành công")
            return df_cf
        except Exception as e2:
            print(f"Lỗi khi lấy báo cáo lưu chuyển tiền tệ lần 2 cho cổ phiếu {stock}: {e2}")
            traceback.print_exc()
            return None

# hàm lấy báo cáo tài chính
def finance_statement(stock):
    finace_data = Finance(symbol=stock, source="VCI")
    finance = finace_data.income_statement(period='year', lang='vi')  # lấy dữ liệu báo cáo tài chính
    # tạo framework chuẩn
    finance_khoiphuc = finance.set_index('Năm').T #transpose data, set header chuẩn
    df_finance = finance_khoiphuc.reset_index().rename(columns = {'index':stock})

    # # xoá rows xấu, ko cần thiết
    df_final = df_finance[~df_finance[stock].isin([
        'CP',
        'Tăng trưởng doanh thu (%)',
        'Doanh thu (đồng)',
        'Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)',
        'Tăng trưởng lợi nhuận (%)',
        'Lãi lỗ trong công ty liên doanh, liên kết'
        ])]

    # đổi tên các row cho đúng formal
    df_đổi_tên = {
        'Doanh thu thuần': 'Doanh thu thuần về bán hàng và cung cấp dịch vụ',
        'Lãi gộp': 'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
        'Thu nhập tài chính': 'Doanh thu hoạt động tài chính',
        'Lãi/Lỗ từ hoạt động kinh doanh': 'Lợi nhuận thuần từ hoạt động kinh doanh',
        'Thu nhập/Chi phí khác': 'Chi phí khác',
        'LN trước thuế': 'Tổng lợi nhuận kế toán trước thuế',
        'Lợi nhuận thuần': 'Lợi nhuận sau thuế thu nhập doanh nghiệp',
        'Cổ đông thiểu số': 'Lợi ích của cổ đông thiểu số',
        'Cổ đông của Công ty mẹ': 'Lợi nhuận sau thuế của cổ đông của Công ty mẹ',
        'Chi phí tiền lãi vay': 'Trong đó: chi phí tiền lãi vay'
    }
    df_final[stock] = df_final[stock].replace(df_đổi_tên)

    # sắp xếp các row cho đúng vị trí
    df_thutudung = [
        'Doanh thu bán hàng và cung cấp dịch vụ',
        'Các khoản giảm trừ doanh thu',
        'Doanh thu thuần về bán hàng và cung cấp dịch vụ',
        'Giá vốn hàng bán',
        'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
        'Doanh thu hoạt động tài chính',
        'Chi phí tài chính',
        'Trong đó: chi phí tiền lãi vay',
        'Lãi/lỗ từ công ty liên doanh',
        'Chi phí bán hàng',
        'Chi phí quản lý DN',
        'Lợi nhuận thuần từ hoạt động kinh doanh',
        'Thu nhập khác',
        'Chi phí khác',
        'Lợi nhuận khác',
        'Tổng lợi nhuận kế toán trước thuế',
        'Chi phí thuế TNDN hiện hành',
        'Chi phí thuế TNDN hoãn lại',
        'Lợi nhuận sau thuế thu nhập doanh nghiệp',
        'Lợi ích của cổ đông thiểu số',
        'Lợi nhuận sau thuế của cổ đông của Công ty mẹ'
    ]
    df_final = df_final.set_index(stock).reindex(df_thutudung).reset_index()

    #xoá các cột ko cần thiết
    columns_to_drop = [str(column) for column in range(2013, 2018)]
    df_final.columns = [str(c).strip() for c in df_final.columns] # loại bỏ khoảng trắng thừa ở tên cột
    current_columns = df_final.columns.tolist() # danh sách cột hiện có trong DataFrame

    columns_exist_to_drop = []
    for col in columns_to_drop:
        if col in current_columns:
            columns_exist_to_drop.append(col)
    df_final = df_final.drop(columns=columns_exist_to_drop, errors='ignore')
    # đổi lại thứ tự các cột từ 2019-2024
    year_today = time.localtime().tm_year
    right_order = [str(year) for year in range(2018, year_today + 1)]
    available_year = []
    for year in right_order:
        if year in df_final.columns:
            available_year.append(year)
    df_final = df_final[[stock] + available_year]
    df_final = df_final.fillna(0)
    return df_final

def price_history(co_phieu):
    try: 
        if not os.path.exists('data_raw/history_price'):  # tạo thư mục
            os.makedirs('data_raw/history_price')
        today = datetime.now().strftime("%Y-%m-%d")
        quote = Quote(symbol=co_phieu, source="VCI")
        data = quote.history(start='2018-01-01', end=today, interval='1D')  # lấy dữ liệu gía lịch sử
        filename_raw = f'{co_phieu}_price_history_{today}.xlsx'
        file_raw_history = 'data_raw/history_price/' + filename_raw
        data.to_excel(file_raw_history, index=False)  # lưu file raw
        df_history_price = pd.read_excel(file_raw_history)
        # căn chỉnh lại cột thời gian
        df_history_price['time'] = df_history_price['time'].dt.strftime('%d-%m-%Y')
    except Exception as e:
        print("Lỗi khi lấy dữ liệu giá lịch sử:", str(e))
        traceback.print_exc()
    return df_history_price

# hàm tính toán các tỉ số tài chính
def financial_ratio_calculation(stock):
    try:
        finace_data = Finance(symbol=stock, source="VCI")
        ratio = finace_data.ratio(period = 'year', lang='vi')  # lấy dữ liệu các tỉ số tài chính

        today = datetime.now().strftime("%d-%m-%y")
        if not os.path.exists('data_raw/ratio_raw'):  # tạo thư mục
            os.makedirs('data_raw/ratio_raw', exist_ok=True)
        filename_raw = f'{stock}_stock_ratio_{today}.xlsx'



        #lưu file raw
        ratio.to_excel(f'data_raw/ratio_raw/{filename_raw}', index= True)
        filename_raw_file = f'data_raw/ratio_raw/{filename_raw}'
    except Exception as e:
        print("Lỗi khi lấy dữ Bliệu tỉ số tài chính:", e)
        traceback.print_exc()
    try:
        df_ratio = pd.read_excel(filename_raw_file, header=1)  # Đọc file với MultiIndex cho cột
        #dọn khoảng trắng thừa
        df_ratio.columns = df_ratio.columns.str.strip()
        # lập danh sách cột cần giữ:
        column_keep = ['Năm']
        #lập danh sách cột bỏ
        columns_drop = ['Unnamed: 0','CP', 'Kỳ']
        #các cột chỉ số:
        ratio_col = [col for col in df_ratio.columns if col not in column_keep and col not in columns_drop]
        df_ratio = df_ratio.melt(id_vars=column_keep, value_vars=ratio_col, var_name='Chỉ số', value_name='Giá trị')

        #pivot lại
        df_ratio = df_ratio.pivot_table(index=column_keep, columns='Chỉ số', values='Giá trị').reset_index()

        #transpose lại
        df_ratio = df_ratio.set_index('Năm').transpose().reset_index()

        #bỏ các chỉ số rác
        drop_ratio = '''
        Chu kỳ tiền
        Chỉ số thanh toán tiền mặt
        EBIT (Tỷ đồng)
        EBITDA (Tỷ đồng)
        Nợ/VCSH
        P/Cash Flow
        Số CP lưu hành (Triệu CP)
        TSCĐ / Vốn CSH
        Tỷ suất cổ tức (%)
        Vòng quay TSCĐ
        Vốn CSH/Vốn điều lệ
        Vốn hóa (Tỷ đồng)
        Đòn bẩy tài chính
        P/B
        P/E
        P/S
        EPS (VND)
        BVPS (VND)
        Số ngày thanh toán bình quân
        Khả năng chi trả lãi vay
        Chỉ số thanh toán nhanh
        ROIC (%)
        Biên EBIT (%)
        ROA (%)
        ROE (%)
        '''
        drop_ratio_list = [item.strip() for item in drop_ratio.strip().splitlines()]
        df_ratio = df_ratio[~df_ratio['Chỉ số'].isin(drop_ratio_list)]

        #đổi tên các mục cần thiết
        rename_dict = {
            '(Vay NH+DH)/VCSH': 'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)',
            'Vòng quay tài sản': 'Vòng quay tổng tài sản',
            'Số ngày tồn kho bình quân':'Thời gian tồn kho bình quân (ngày)',
            'Số ngày thu tiền bình quân': 'Thời gian thu tiền khách hàng bình quân (ngày)',
            'Chỉ số thanh toán hiện thời': 'Tỷ số thanh toán hiện hành (ngắn hạn)',
        }
        df_ratio['Chỉ số'] = df_ratio['Chỉ số'].replace(rename_dict)

        # đổi tên cột năm thành string các số nguyên
        columns = []
        for c in df_ratio.columns:
            if isinstance(c, float):
                columns.append(str(int(c)).strip())
            else:
                columns.append(str(c).strip())
        df_ratio.columns = columns

        #xoá các cột ko cần thiết
        columns_drop = [str(c) for c in range(2013, 2018)]  # bỏ các năm 2013-2017
        to_drop = []
        for c in df_ratio.columns:
            if str(c).strip() in columns_drop:
                to_drop.append(c)
        df_ratio = df_ratio.drop(columns=to_drop, errors='ignore',axis = 1).set_index(df_ratio.columns[0])


        # nhập dữ liệu từ báo cáo tài chính

        df_kqkd = finance_statement(stock)
        df_lctt = cash_flow_statement(stock)
        df_balance = balance_sheet(stock)
        df_price = price_history(stock)

        df_kqkd = finance_statement(stock).set_index(df_kqkd.columns[0])
        df_lctt = cash_flow_statement(stock).set_index(df_lctt.columns[0])
        df_balance = balance_sheet(stock).set_index(df_balance.columns[0])
        df_price = price_history(stock).set_index(df_price.columns[0])


        #xoá khoảng trắng các tên cột, index
        df_price = df_price.reset_index()
        df_kqkd.columns = df_kqkd.columns.astype(str).str.strip()
        df_balance.columns = df_balance.columns.astype(str).str.strip()
        df_price.columns = df_price.columns.astype(str).str.strip()
        df_lctt.columns = df_lctt.columns.astype(str).str.strip()

        #________________________tính toán các chỉ số còn thiếu________________________
        thu_tu_dung = [
            "Nhóm chỉ số Định giá",
            "EPS",
            "BVPS",
            "P/E",
            "P/B",
            "P/S",
            "EV/EBITDA",
            "Nhóm chỉ số Sinh lợi",
            "Biên EBIT (%)",
            "Biên lợi nhuận gộp (%)",
            "Biên lợi nhuận ròng (%)",
            "Tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA) (%)",
            "Tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA) (%)",
            "Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE) (%)",
            "Nhóm chỉ số Tăng trưởng",
            "Tăng trưởng doanh thu thuần (%)",
            "Tăng trưởng lợi nhuận sau thuế (%)",
            "Nhóm chỉ số Thanh khoản",
            "Tỷ số thanh toán hiện hành (ngắn hạn)",
            "Chỉ số thanh toán nhanh",
            "Khả năng chi trả lãi vay",
            "Nhóm chỉ số Hiệu quả hoạt động",
            "Thời gian thu tiền khách hàng bình quân (ngày)",
            "Thời gian tồn kho bình quân (ngày)",
            "Vòng quay hàng tồn kho",
            "Vòng quay tổng tài sản",
            "Số ngày trả cho nhà cung cấp (ngày)",
            "Vòng quay trả cho nhà cung cấp",
            "Chu kỳ chuyển đổi tiền mặt (CCC) (ngày)",
            "Nhóm chỉ số Đòn bẩy tài chính",
            "Tỷ số Nợ vay trên Vốn chủ sở hữu (%)",
            "Tỷ số Nợ vay trên Tổng tài sản (%)",
            "Nhóm chỉ số Dòng tiền",
            "Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)",
            "Dòng tiền từ HĐKD trên Tổng tài sản (%)",
            "Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)",
            "Dòng tiền từ HĐKD trên mỗi cổ phần"
        ]


        #tính EPS
        von_dieu_le = df_balance.loc['1. Vốn đầu tư của chủ sở hữu']
        so_co_phieu = von_dieu_le / 10000 # giả sử mệnh giá 10k/cp
        loi_nhuan_rong_nam = df_kqkd.loc['Lợi nhuận sau thuế của cổ đông của Công ty mẹ']
        loi_nhuan_rong_nam = pd.to_numeric(loi_nhuan_rong_nam, errors='coerce')
        so_co_phieu = pd.to_numeric(so_co_phieu, errors='coerce')
        eps = (loi_nhuan_rong_nam / so_co_phieu).round(2)
        df_ratio.loc['EPS']= eps

        #tính P/E
        df_price['time'] = pd.to_datetime(df_price['time'], format = '%d-%m-%Y')
        df_price = df_price.set_index('time')
        gia_cuoi_nam_series = df_price['close'].resample('Y').last() #giá cuối năm
        gia_cuoi_nam_series= gia_cuoi_nam_series.iloc[:-1] #bỏ năm hiện tại chưa kết thúc
        #set index là năm, đổi index thành str
        gia_cuoi_nam_series.index = gia_cuoi_nam_series.index.year.astype(str)
        # nhân với 1,000 
        gia_cuoi_nam = gia_cuoi_nam_series * 1000
        von_hoa = gia_cuoi_nam * so_co_phieu
        pe_ratio = (von_hoa/loi_nhuan_rong_nam).round(2)
        df_ratio.loc['P/E'] = pe_ratio

        #tính BVPS
        loi_ich_co_dong_thieu_so = df_balance.loc['14. Lợi ích của cổ đông không kiểm soát']
        von_chu_so_huu = df_balance.loc['I. Vốn chủ sở hữu']
        von_chu_so_huu = pd.to_numeric(von_chu_so_huu, errors='coerce')
        bvps = (von_chu_so_huu  / so_co_phieu).round(2)
        df_ratio.loc['BVPS'] = bvps
        #tính P/B
        pb_ratio = (von_hoa / von_chu_so_huu).round(2)
        df_ratio.loc['P/B'] = pb_ratio

        #tính P/S
        doanh_thu_thuan_nam = df_kqkd.loc['Doanh thu thuần về bán hàng và cung cấp dịch vụ']
        ps_ratio = (von_hoa/ doanh_thu_thuan_nam).round(2)
        df_ratio.loc['P/S'] = ps_ratio


        #tăng trưởng doanh thu thuần và lợi nhuận sau thuế CĐ công ty mẹ
        doanh_thu_nam_series = df_kqkd.loc['Doanh thu thuần về bán hàng và cung cấp dịch vụ']
        growth_revenue_series = doanh_thu_nam_series.pct_change(periods=1).fillna(0)

        loi_nhuan_me_series = df_kqkd.loc['Lợi nhuận sau thuế của cổ đông của Công ty mẹ']
        growth_profit_series = loi_nhuan_me_series.pct_change(periods=1).fillna(0)


        df_ratio.loc['Tăng trưởng doanh thu thuần (%)'] = growth_revenue_series
        df_ratio.loc['Tăng trưởng lợi nhuận sau thuế (%)'] = growth_profit_series

        # vòng quay trả cho nhà cung cấp và thời gian trả cho nhà cung cấp
        cogs = df_kqkd.loc['Giá vốn hàng bán']  
        payable_begin = df_balance.loc['3. Phải trả người bán ngắn hạn'].shift(1)
        payable_end = df_balance.loc['3. Phải trả người bán ngắn hạn']
        average_payable = (payable_begin + payable_end) / 2
        vong_quay_tra_cho_nha_cung_cap = (cogs / average_payable) * -1
        vong_quay_tra_cho_nha_cung_cap = pd.to_numeric(vong_quay_tra_cho_nha_cung_cap, errors='coerce')
        so_ngay_tra_cho_nha_cung_cap = (365 / vong_quay_tra_cho_nha_cung_cap).round(2)
        df_ratio.loc['Số ngày trả cho nhà cung cấp (ngày)'] = so_ngay_tra_cho_nha_cung_cap.fillna(0)
        df_ratio.loc['Vòng quay trả cho nhà cung cấp'] = vong_quay_tra_cho_nha_cung_cap.round(2).fillna(0)

        #tính chu kỳ chuyển đổi tiền mặt
        if 'Thời gian tồn kho bình quân (ngày)' in df_ratio.index:
            so_ngay_ton_kho = df_ratio.loc['Thời gian tồn kho bình quân (ngày)']
            so_ngay_thu_tien_khach_hang = df_ratio.loc['Thời gian thu tiền khách hàng bình quân (ngày)']
            CCC = (so_ngay_ton_kho + so_ngay_thu_tien_khach_hang - so_ngay_tra_cho_nha_cung_cap).fillna(0).round(0)
            df_ratio.loc['Chu kỳ chuyển đổi tiền mặt (CCC) (ngày)'] = CCC

        #tính khả năng chi trả lãi vay
        loi_nhuan_truoc_thue = df_kqkd.loc['Tổng lợi nhuận kế toán trước thuế']
        chi_phi_lai = df_kqkd.loc['Trong đó: chi phí tiền lãi vay'] * -1
        ebit = loi_nhuan_truoc_thue + chi_phi_lai
        khả_năng_chi_tra_lai_vay = (ebit / chi_phi_lai).round(2)
        df_ratio.loc['Khả năng chi trả lãi vay'] = khả_năng_chi_tra_lai_vay

        # chỉ số thanh toán nhanh
        ts_ngan_han = df_balance.loc['A. Tài sản lưu động và đầu tư ngắn hạn']
        ts_ngan_han = pd.to_numeric(ts_ngan_han, errors='coerce')
        hang_ton_kho = df_balance.loc['IV. Tổng hàng tồn kho']
        hang_ton_kho = pd.to_numeric(hang_ton_kho, errors='coerce')
        ts_ngan_han_tru_hang_ton_kho = ts_ngan_han - hang_ton_kho
        no_ngan_han = df_balance.loc['I. Nợ ngắn hạn']
        no_ngan_han = pd.to_numeric(no_ngan_han, errors='coerce')
        chi_so_thanh_toan_nhanh = (ts_ngan_han_tru_hang_ton_kho / no_ngan_han).round(2)
        df_ratio.loc['Chỉ số thanh toán nhanh'] = chi_so_thanh_toan_nhanh

        # tỷ số nợ vay trên tổng tài sản
        tong_tai_san = df_balance.loc['TỔNG CỘNG TÀI SẢN']
        tong_tai_san = pd.to_numeric(tong_tai_san, errors='coerce')
        nợ_vay_ngắn_hạn = df_balance.loc['1. Vay và nợ thuê tài chính ngắn hạn'] + df_balance.loc['2. Vay và nợ dài hạn đến hạn phải trả'] 
        nợ_vay_dài_hạn = df_balance.loc['6. Vay và nợ thuê tài chính dài hạn'] + df_balance.loc['7. Trái phiếu chuyển đổi']
        tong_no_vay = nợ_vay_ngắn_hạn + nợ_vay_dài_hạn
        tong_no_vay = pd.to_numeric(tong_no_vay, errors='coerce')
        ty_so_no_vay_tren_tong_tai_san = ((tong_no_vay / tong_tai_san)*100).round(2)
        df_ratio.loc['Tỷ số Nợ vay trên Tổng tài sản (%)'] = ty_so_no_vay_tren_tong_tai_san

        # CF margin 
        cfo_series = df_lctt.loc['Lưu chuyển tiền thuần từ hoạt động kinh doanh']
        cfo_series = pd.to_numeric(cfo_series, errors='coerce')
        # Dòng tiền từ HĐKD trên Doanh thu thuần (CF margin)
        cf_margin = (cfo_series / doanh_thu_thuan_nam).round(4)
        df_ratio.loc['Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)'] = cf_margin

        # Dòng tiền từ HĐKD trên Tổng tài sản (CFROA)
        cfroa = (cfo_series / tong_tai_san).round(4)
        df_ratio.loc['Dòng tiền từ HĐKD trên Tổng tài sản (%)'] = cfroa

        # Dòng tiền từ HĐKD trên Vốn chủ sở hữu (CFROE)
        cfroe = (cfo_series / von_chu_so_huu).round(4)
        df_ratio.loc['Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)'] = cfroe

        # Dòng tiền từ HĐKD trên mỗi cổ phần (CPS)
        cps = (cfo_series / so_co_phieu).round(2)
        df_ratio.loc['Dòng tiền từ HĐKD trên mỗi cổ phần'] = cps

        #biên EBIT
        biên_ebit = ((ebit / doanh_thu_thuan_nam) * 100).round(2) 
        df_ratio.loc['Biên EBIT (%)'] = biên_ebit

        #Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)
        asset_end = df_balance.loc['TỔNG CỘNG TÀI SẢN']
        asset_end = pd.to_numeric(asset_end, errors='coerce')
        asset_begin = asset_end.shift(1)
        asset_avg = (asset_begin + asset_end) / 2
        nợ_ngắn_hạn_end = df_balance.loc['I. Nợ ngắn hạn']
        nợ_ngắn_hạn_end = pd.to_numeric(nợ_ngắn_hạn_end, errors='coerce')
        nợ_ngắn_hạn_begin = nợ_ngắn_hạn_end.shift(1)
        nợ_ngắn_hạn_avg = (nợ_ngắn_hạn_begin + nợ_ngắn_hạn_end) / 2
        vốn_dài_hạn_avg = asset_avg - nợ_ngắn_hạn_avg
        roce = (ebit / vốn_dài_hạn_avg).round(4).fillna(0) * 100
        df_ratio.loc['Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE) (%)'] = roce

        #tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA)
        lnst = df_kqkd.loc['Lợi nhuận sau thuế thu nhập doanh nghiệp']
        lnst = pd.to_numeric(lnst, errors='coerce')
        roaa = (lnst / asset_avg).round(4).fillna(0) * 100
        df_ratio.loc['Tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA) (%)'] = roaa

        #tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA)
        vốn_chủ_sở_hữu_end = df_balance.loc['B. Nguồn vốn chủ sở hữu']
        vốn_chủ_sở_hữu_end = pd.to_numeric(vốn_chủ_sở_hữu_end, errors='coerce')
        vốn_chủ_sở_hữu_begin = vốn_chủ_sở_hữu_end.shift(1)
        vốn_chủ_sở_hữu_avg = (vốn_chủ_sở_hữu_begin + vốn_chủ_sở_hữu_end) / 2
        roe = (loi_nhuan_me_series / vốn_chủ_sở_hữu_avg).round(4).fillna(0) * 100
        df_ratio.loc['Tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA) (%)'] = roe

        # sửa lỗi thiếu index 1 số cổ phiếu (update)
        for index in thu_tu_dung:
            if not index in df_ratio.index:
                df_ratio.loc[index] = None
            
        #làm tròn các chỉ số đến hàng thập phân thứ 2
        row_round_2 = [
            'EV/EBITDA',
            'Biên lợi nhuận gộp (%)',
            'Biên lợi nhuận ròng (%)',
            'Tăng trưởng doanh thu thuần (%)',
            'Tăng trưởng lợi nhuận sau thuế (%)',
            'Tỷ số thanh toán hiện hành (ngắn hạn)',
            'Vòng quay hàng tồn kho',
            'Vòng quay tổng tài sản',
            'Thời gian thu tiền khách hàng bình quân (ngày)',
            'Thời gian tồn kho bình quân (ngày)',
            'Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)',
            'Dòng tiền từ HĐKD trên Tổng tài sản (%)',
            'Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)',
            'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)'
            
        ]
        # các hàng cần phải nhân thêm cả 100
        round_100 = ['Biên lợi nhuận gộp (%)',
            'Biên lợi nhuận ròng (%)',
            'Tăng trưởng doanh thu thuần (%)',
            'Tăng trưởng lợi nhuận sau thuế (%)',
            'Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)',
            'Dòng tiền từ HĐKD trên Tổng tài sản (%)',
            'Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)',
            'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)'
            ]
            
        for row in row_round_2:
            if df_ratio.loc[row].isnull().all():
                continue
            if row in round_100:
                df_ratio.loc[row] = (df_ratio.loc[row] * 100).round(2)
            else:
                df_ratio.loc[row] = df_ratio.loc[row].round(2)

        # thêm tên định danh các nhóm chỉ số
        nhom_chi_so = [
            "Nhóm chỉ số Định giá",
            "Nhóm chỉ số Sinh lợi",
            "Nhóm chỉ số Tăng trưởng",
            "Nhóm chỉ số Thanh khoản",
            "Nhóm chỉ số Hiệu quả hoạt động",
            "Nhóm chỉ số Đòn bẩy tài chính",
            "Nhóm chỉ số Dòng tiền"
        ]


        for group in nhom_chi_so:
            df_ratio.loc[group] = np.nan
            
        df_ratio = df_ratio.reindex(thu_tu_dung).reset_index()
        return df_ratio
    except Exception as e:
        print("Lỗi khi xử lý dữ liệu tỉ số tài chính:", e)
        traceback.print_exc()
        return None





