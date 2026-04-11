import streamlit as st
import polars as pl
from pathlib import Path

# Tìm file CSV nằm ngay cạnh file app.py này
csv_path = Path(__file__).parent / "data_qmj.csv"

@st.cache_data
def load_data():
    """Hàm này chỉ chạy đúng 1 lần khi bác mở web hoặc F5"""
    if csv_path.exists():
        return pl.read_csv(csv_path)
    return None

# --- BÂY GIỜ GỌI HÀM NHƯ BÌNH THƯỜNG ---
df = load_data()

st.title("Demo Dữ liệu QMJ")

if df is not None:
    # Đoạn logic filter của bác đặt ở dưới này...
    st.header(" bảng danh sách dữ liệu cổ phiếu chấm điểm theo QMJ", divider="gray")
    st.dataframe(df)
select_box = [1,2,3,4,5]  # Đây là ví dụ, bác có thể thay bằng list các kỳ báo cáo thực tế
selected_q = st.selectbox("Chọn Kỳ Báo Cáo:", select_box)