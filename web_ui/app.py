import streamlit as st
import polars as pl
from pathlib import Path

# ==========================================
# 1. DATA PROCESSING (XỬ LÝ DỮ LIỆU)
# ==========================================
@st.cache_data
def transform_data():
    csv_path = Path(__file__).parent / "data_qmj.csv"
    if not csv_path.exists():
        return None
    df = pl.read_csv(csv_path)
    df = df.with_columns(
        pl.concat_str([
            pl.lit("Quý"), pl.lit(" "), pl.col("quarter"), pl.lit(" - "), pl.col("year")
        ]).alias("ui_label")
    )
    return df

def _get_quarters_for_selectbox(df: pl.DataFrame) -> list[str]:
    unique_quarters = (
        df.select(["ui_label", "absolute_quarter"])
        .unique()
        .sort("absolute_quarter", descending=True)
    )
    return unique_quarters["ui_label"].to_list()

def _get_top_qmj_rank(df_filtered_by_quarter: pl.DataFrame) -> int:
    return int(df_filtered_by_quarter["qmj_rank"].max()) # type: ignore
def _get_latest_update_time(df: pl.DataFrame) -> str:
    """Lấy thời gian cập nhật mới nhất từ cột obt_updated_at"""
    latest_time = str(df["obt_updated_at"].max())
    return latest_time[:19]
# ==========================================
# 2. UI COMPONENTS (CÁC THÀNH PHẦN GIAO DIỆN)
# ==========================================
def render_filters(df: pl.DataFrame):
    """Hàm này chỉ chuyên lo việc vẽ bộ lọc và trả về dữ liệu đã lọc"""
    list_q = _get_quarters_for_selectbox(df)
    
    col1, col2 = st.columns(2)
    
    # Cột 1: Chọn Quý
    with col1:
        selected_q = st.selectbox("Chọn Kỳ Báo Cáo:", options=list_q)
        df_filtered_by_q = df.filter(pl.col("ui_label") == selected_q)
        
    with col2:
        # Tạo một từ điển map giữa Tên hiển thị và Cột tương ứng trong data
        # desc=True nghĩa là điểm cao xếp trên, desc=False là hạng thấp (số 1) xếp trên
        criteria_dict = {
            "Hạng QMJ": {"col": "qmj_rank", "desc": False},
            "Đà tăng trưởng (Top Momentum)": {"col": "z_momentum_recent", "desc": True},
            "Định giá hấp dẫn (Top Value)": {"col": "z_value_recent", "desc": True}
        }
        selected_criteria = st.selectbox("🏆 Chọn Tiêu chí Xếp hạng:", options=list(criteria_dict.keys()))
    
    max_rank = _get_top_qmj_rank(df_filtered_by_q)
    selected_top_n = st.slider(
        "Hiển thị Top cổ phiếu (theo Hạng QMJ):",
        min_value=1,
        max_value=max_rank,
        value=max_rank, 
        step=1
    )
    sort_col = criteria_dict[selected_criteria]["col"]
    is_desc = criteria_dict[selected_criteria]["desc"]
        
    # Lọc lần cuối theo Rank
    df_final = (
        df_filtered_by_q
        .filter(pl.col("qmj_rank") <= selected_top_n)
        .sort(sort_col, descending=is_desc) # Hạng 1 lên đầu
    )
    
    return df_final, selected_q

def render_main_content(df: pl.DataFrame, selected_q: str, updated_time: str):
    """Hàm này chỉ chuyên lo việc vẽ bảng dữ liệu (đã được làm đẹp)"""
    st.header(f"Báo cáo QMJ - {selected_q}", divider="gray")
    st.caption(f"Thời gian cập nhật: {updated_time}")
    view_config = {
        "year": None, "quarter": None, "absolute_quarter": None,
        "obt_updated_at": None, "obt_invocation_id": None,
        "ui_label": st.column_config.TextColumn("Kỳ báo cáo (Period)", width="medium"),
        "ticker": st.column_config.TextColumn("Mã CK (Ticker)", width="small"),
        "company_name": "Tên Công ty (Company Name)",
        "exchange": "Sàn (Exchange)",
        "industry_group": "Nhóm Ngành (Industry Group)",
        "sector_detail": "Lĩnh vực (Sector)",
        "qmj_score": st.column_config.ProgressColumn(
            "Điểm QMJ (QMJ Score)", help="Quality Minus Junk Total Score",
            format="%.2f", min_value=0, max_value=1
        ),
        "qmj_profitability": st.column_config.NumberColumn("Lợi nhuận (Profitability)", format="%.2f"),
        "qmj_growth": st.column_config.NumberColumn("Tăng trưởng (Growth)", format="%.2f"),
        "qmj_safety": st.column_config.NumberColumn("An toàn (Safety)", format="%.2f"),
        "qmj_rank": "Hạng (Rank)",
        "current_market_cap": st.column_config.NumberColumn("Vốn hóa (Market Cap - Bn)", format="%.0f"),
        "avg_volume_3m": st.column_config.NumberColumn("KLGD TB 3T (Avg Vol 3M)", format="%.0f"),
        "quarter_market_cap": st.column_config.NumberColumn("Vốn hóa chốt Quý", format="%.0f"),
        "quarter_shares_outstanding": st.column_config.NumberColumn("CP Lưu hành chốt Quý", format="%.0f"),
        "z_value_historical": st.column_config.NumberColumn("Định giá lịch sử (Value Z)", format="%.2f"),
        "z_momentum_historical": st.column_config.NumberColumn("Đà tăng lịch sử (Momentum Z)", format="%.2f"),
        "z_value_recent": st.column_config.NumberColumn("Định giá (Value Z)", format="%.2f"),
        "z_momentum_recent": st.column_config.NumberColumn("Đà tăng (Momentum Z)", format="%.2f"),
    }

    st.dataframe(df, column_config=view_config, use_container_width=True, hide_index=True)

# ==========================================
# 3. ORCHESTRATOR (NHẠC TRƯỞNG ĐIỀU PHỐI)
# ==========================================
def main():
    st.set_page_config(page_title="QMJ Dashboard", layout="wide")
    st.title("Demo Dữ liệu QMJ")
    st.header("Bảng danh sách dữ liệu cổ phiếu chấm điểm theo QMJ", divider="gray")

    # 1. Lấy dữ liệu gốc
    df_raw = transform_data()
    
    if df_raw is not None:
        # 2. Đưa dữ liệu qua bộ lọc (Lấy về dữ liệu đã lọc và quý đang chọn)
        updated_time = _get_latest_update_time(df_raw)
        df_filtered, selected_q = render_filters(df_raw)
        
        # 3. Đưa dữ liệu đã lọc lên bảng vẽ
        render_main_content(df_filtered, selected_q, updated_time)
    else:
        st.error("⚠️ Không tìm thấy file dữ liệu (data_qmj.csv)!")




if __name__ == "__main__":
    main()