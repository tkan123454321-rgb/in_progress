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

def _get_top_qmj_rank(df_filtered_by_quarter: pl.DataFrame) -> int:
    max_rank = int(df_filtered_by_quarter["qmj_rank"].max()) # type: ignore
    return max_rank

def render_main_content(df, selected_q):
    st.header(f"Báo cáo QMJ - {selected_q}", divider="gray")
    
    view_config = {
        "year": None,
        "quarter": None,
        "absolute_quarter": None,
        "obt_updated_at": None,
        "obt_invocation_id": None,
        
        "ui_label": st.column_config.TextColumn("Kỳ báo cáo (Period)", width="medium"),
        
        "ticker": st.column_config.TextColumn("Mã CK (Ticker)", width="small"),
        "company_name": "Tên Công ty (Company Name)",
        "exchange": "Sàn (Exchange)",
        "industry_group": "Nhóm Ngành (Industry Group)",
        "sector_detail": "Lĩnh vực (Sector)",
        
        # --- ĐIỂM SỐ QMJ (Dùng Progress bar cho sang) ---
        "qmj_score": st.column_config.ProgressColumn(
            "Điểm QMJ (QMJ Score)", 
            help="Quality Minus Junk Total Score",
            format="%.2f", 
            min_value=0, 
            max_value=1
        ),
        "qmj_profitability": st.column_config.NumberColumn("Lợi nhuận (Profitability)", format="%.2f"),
        "qmj_growth": st.column_config.NumberColumn("Tăng trưởng (Growth)", format="%.2f"),
        "qmj_safety": st.column_config.NumberColumn("An toàn (Safety)", format="%.2f"),
        "qmj_rank": "Hạng (Rank)",
        
        # --- CHỈ SỐ THỊ TRƯỜNG ---
        "current_market_cap": st.column_config.NumberColumn("Vốn hóa (Market Cap - Bn)", format="%.0f"),
        "avg_volume_3m": st.column_config.NumberColumn("KLGD TB 3T (Avg Vol 3M)", format="%.0f"),
        "quarter_market_cap": st.column_config.NumberColumn(
            "Vốn hóa chốt Quý (Quarter Mkt Cap)", 
            help="Vốn hóa tính tại thời điểm kết thúc quý báo cáo",
            format="%.0f"
        ),
        "quarter_shares_outstanding": st.column_config.NumberColumn(
            "CP Lưu hành chốt Quý (Quarter Shares)", 
            help="Số lượng cổ phiếu lưu hành tại thời điểm kết thúc quý",
            format="%.0f"
        ),
        
        # --- CÁC CHỈ SỐ ĐỊNH GIÁ & ĐÀ TĂNG ---
        "z_value_historical": st.column_config.NumberColumn(
            "Định giá lịch sử (Value Z - Historical)", 
            help="Điểm định giá trung bình trong quá khứ",
            format="%.2f"
        ),
        "z_momentum_historical": st.column_config.NumberColumn(
            "Đà tăng lịch sử (Momentum Z - Historical)", 
            help="Điểm đà tăng trung bình trong quá khứ",
            format="%.2f"
        ),
        "z_value_recent": st.column_config.NumberColumn("Định giá (Value Z)", format="%.2f"),
        "z_momentum_recent": st.column_config.NumberColumn("Đà tăng (Momentum Z)", format="%.2f"),
    }

    # Nếu bác còn các cột khác trong ảnh (như shares_outstanding) mà không muốn hiện, 
    # bác cứ thêm tên cột đó vào list và gán = None ở trên nhé.

    st.dataframe(
        df,
        column_config=view_config,
        use_container_width=True,
        hide_index=True,
    )




st.title("Demo Dữ liệu QMJ")

    # Đoạn logic filter của bác đặt ở dưới này...
df = transform_data()
st.header(" bảng danh sách dữ liệu cổ phiếu chấm điểm theo QMJ", divider="gray")
list_q = _get_quarters_for_selectbox(df)
col1, col2 = st.columns([1, 2])
with col1:
    selected_q = st.selectbox("Chọn Kỳ Báo Cáo:", options=list_q)
    df_filtered = df.filter(pl.col("ui_label") == selected_q)
with col2:
    selected_top_n = st.slider(
            "Hiển thị Top cổ phiếu (theo Hạng QMJ):",
            min_value=1,
            max_value=_get_top_qmj_rank(df_filtered),
            value=_get_top_qmj_rank(df_filtered), 
            step=1
        )
df = (
        df_filtered
        .filter(pl.col("qmj_rank") <= selected_top_n)
        .sort("qmj_rank") # Đảm bảo rank 1 luôn nằm trên cùng
    )
render_main_content(df, selected_q)
