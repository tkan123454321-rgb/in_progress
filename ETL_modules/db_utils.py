import os
from sqlalchemy import text
import jinja2
from ETL_modules.configs import table_cleanup_config
import threading
import requests




def db_cleanup(engine):
    # 1. Đường dẫn đến file template Jinja (file có cái logo ngầu ấy)
    cur_dir = os.getcwd()
    file_path_jinja = os.path.join(cur_dir, 'SQL', 'stored_procedure.sql.jinja2')
    
    if not os.path.exists(file_path_jinja):
        print(f"❌ Không tìm thấy file template: {file_path_jinja}")
        return

    # 2. Đọc nội dung file template
    with open(file_path_jinja, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    # 3. Khởi tạo "Máy đúc" Jinja
    # (Dùng Template object của Jinja thay vì .format của Python)
    jinja_template = jinja2.Template(template_content)

    try:
        with engine.begin() as conn:
            # 4. Duyệt qua từng bảng trong Config
            for conf in table_cleanup_config:
                table = conf['table_name']
                cols = conf['compare_cols']
                
                print(f"🛠  Đang đúc SQL dọn dẹp cho bảng: {table}...")
                
                # --- PHÉP THUẬT NẰM Ở ĐÂY ---
                # Render: Nạp dữ liệu vào khuôn -> Ra câu SQL thuần
                sql_query = jinja_template.render(
                    table_name=table,
                    compare_columns=cols
                )
                
                # (Optional) In ra xem thử câu SQL nó sinh ra thế nào
                # print(sql_query) 

                # 5. Bắn thẳng câu SQL vào DB (Không cần CALL procedure nữa)
                conn.execute(text(sql_query))
                print(f"✅ Đã dọn sạch bảng {table}!")
                
    except Exception as e:
        print(f"❌ Lỗi khi chạy Jinja: {e}")


def get_session():
    # Kiểm tra xem ông thợ này đã có đồ nghề (Session) trong túi chưa?
    thread_local = threading.local()
    if not hasattr(thread_local, "session"):
        # Nếu chưa -> Cấp mới 1 cái Session
        thread_local.session = requests.Session()
        auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
        
        # --- QUAN TRỌNG: CẢI TRANG ---
        # Đeo mặt nạ vào để Server tưởng mình là trình duyệt Chrome xịn, chứ không phải Python script
        thread_local.session.headers.update({
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://fireant.vn/",
            "authorization": auth_token}
        )
    return thread_local.session
