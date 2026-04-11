import streamlit as st
import polars as pl
from pathlib import Path




@st.cache_data
def transform_data():
    csv_path = Path(__file__).parent / "data_qmj.csv"
    df = pl.read_csv(csv_path)
    if df is None:
        return None
    df = df.with_columns( # type: ignore
        pl.concat_str([
            pl.lit("Quý"),pl.lit(" "), pl.col("quarter"), pl.lit(" - "), pl.col("year")
        ]).alias("ui_label")
    )
    return df

def _get_quarters_for_selectbox(df: pl.DataFrame) -> list[str]:
    # Lấy danh sách quý duy nhất (mới nhất lên đầu)
    unique_quarters = (
        df.select(["ui_label", "absolute_quarter"])
        .unique()
        .sort("absolute_quarter", descending=True)
    )
    list_quarters = unique_quarters["ui_label"].to_list()
    return list_quarters




st.title("Demo Dữ liệu QMJ")

    # Đoạn logic filter của bác đặt ở dưới này...
df = transform_data()
st.header(" bảng danh sách dữ liệu cổ phiếu chấm điểm theo QMJ", divider="gray")
list_q = _get_quarters_for_selectbox(df)
    
    # Thiết lập mặc định là quý đầu tiên trong list (quý mới nhất)
selected_q = st.selectbox("Chọn Kỳ Báo Cáo:", options=list_q)

    # 3. LOGIC LỌC: Đây là chỗ then chốt bác đang cần
    # Khi người dùng chọn selected_q, Polars sẽ lọc lại bảng ngay lập tức
df_filtered = df.filter(pl.col("ui_label") == selected_q)
st.dataframe(df_filtered)
