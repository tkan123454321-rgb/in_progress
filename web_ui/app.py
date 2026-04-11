import streamlit as st
import polars as pl
from pathlib import Path

# Tìm file CSV nằm ngay cạnh file app.py này
csv_path = Path(__file__).parent / "data_qmj.csv"

st.title("Demo Dữ liệu QMJ")

# Đọc và hiện bảng
if csv_path.exists():
    df = pl.read_csv(csv_path)
    st.write(f"Đã load thành công {len(df)} dòng.")
    st.dataframe(df)
else:
    st.error("Không tìm thấy file data_qmj.csv ở cùng thư mục!")