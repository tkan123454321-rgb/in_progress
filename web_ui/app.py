import streamlit as st
import polars as pl
from pathlib import Path
from datetime import datetime

# ==========================================
# 1. DATA PROCESSING (XỬ LÝ DỮ LIỆU)
# ==========================================
@st.cache_data
def transform_data():
    csv_path = Path(__file__).parent / "data_qmj.csv"
    if not csv_path.exists():
        return None
    df = pl.read_csv(csv_path)
    df = df.with_columns([
        pl.concat_str([
            pl.lit("Quý"), pl.lit(" "), pl.col("quarter"), pl.lit(" - "), pl.col("year")
        ]).alias("ui_label"),
        (pl.col("current_market_cap") / 1_000_000_000).alias("current_market_cap"),
        (pl.col("quarter_market_cap") / 1_000_000_000).alias("quarter_market_cap")
    ])
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
    dt_obj = datetime.strptime(latest_time[:19], "%Y-%m-%dT%H:%M:%S")
    return dt_obj.strftime("%d-%m-%y %H:%M:%S")
# ==========================================
# 2. UI COMPONENTS (CÁC THÀNH PHẦN GIAO DIỆN)
# ==========================================
def render_filters(df: pl.DataFrame):
    """Hàm này chỉ chuyên lo việc vẽ bộ lọc và trả về dữ liệu đã lọc"""
    list_q = _get_quarters_for_selectbox(df)
    
    col1, col2, col3 = st.columns([1,1,1])
    
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
            "Định giá (Top Value)": {"col": "z_value_recent", "desc": True}
        }
        help_text = """
        **Giải thích các Tiêu chí Xếp hạng:**
        * **Hạng QMJ**: Xếp hạng các cổ phiếu có nền tảng tài chính bền vững nhất dựa trên điểm Z-score tổng hợp từ các nhân tố Profitability, Growth và Safety.
        * **Đà tăng trưởng**: Xác định các cổ phiếu có sức mạnh giá và đà tăng trưởng dòng tiền dẫn dắt thị trường thông qua đo lường xu hướng tiếp diễn.
        * **Định giá hấp dẫn**: Sàng lọc các cổ phiếu có mức định giá thấp hoặc hấp dẫn so với giá trị nội tại.
        """

        # 3. Thêm tham số help vào selectbox
        selected_criteria = st.selectbox(
            "Chọn Tiêu chí Xếp hạng:", 
            options=list(criteria_dict.keys()),
            help=help_text  
        )
    with col3:
        search_ticker = st.text_input("Tìm nhanh mã CK:", placeholder="VD: VNM, HPG...").upper().strip()
    
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
    if search_ticker:
        df_final = df_filtered_by_q.filter(pl.col("ticker").str.contains(search_ticker)).sort(sort_col, descending=is_desc)
    else:
        df_final = (
            df_filtered_by_q
            .filter(pl.col("qmj_rank") <= selected_top_n)
            .sort(sort_col, descending=is_desc) # Hạng 1 lên đầu
        )
    
    return df_final, selected_q, selected_top_n, selected_criteria

def _introduction():
    with st.expander(" Tại sao hệ thống này ra đời? (The 'Why' behind the Product) "):
        
        st.markdown("""
        ### 1. Câu chuyện của mình: khi kiến thức tài chính là không đủ.
        Đầu năm 3 đại học, sau khi nhận email báo đỗ CFA Level 1 với điểm số 1755/1900, mình từng nghĩ: Mình sẽ chọn một mã cổ phiếu, áp dụng đống công thức định giá xịn vừa học được, viết một cái report thật chất lượng 20-30 trang PDF rồi tự tin gửi CV ứng tuyển vào khối phân tích của các công ty chứng khoán hoặc các quỹ đầu tư thì bao pass cv và vào thẳng phỏng vấn.

        Nhưng hiện thực nó không vận hành như vậy. Bắt tay vào làm thì mới thấy khó khăn xuất hiện dần dần.
        Việc chọn ra 1 mã cổ phiếu bất kỳ để phân tích, Lôi báo cáo tài chính ra để xuất excel xong bóc tách, xong lại "kéo Excel" để tính P/E, P/B, ROE... cho một công ty sẽ mất vô cùng nhiều thời gian. Điều tệ nhất là hì hục làm mới nhận ra mã đó vốn dĩ không có tiềm năng dài hạn, nền tảng của nó không đủ tốt thì lúc đó chắc chắn công sức như muối bỏ bể. Nó giống như việc đãi cát tìm vàng trong danh sách 1,600 mã cổ phiếu trên sàn chứng khoán Việt Nam.

        Sự bế tắc đó làm mình nhận ra: nếu như đi theo con đường truyền thống bằng cách chọn từng mã phân tích thủ công thì sẽ vô cùng may rủi và tốn thời gian.
        Do đó, thay vì cố chấp làm một chuyên viên phân tích tài chính chọn mã bằng sức người, mình chọn lùi lại 1 bước để nhìn toàn cảnh. Mình đã chọn trở thành một data engineer để xây một hệ thống pipeline dữ liệu sàng lọc tự động, khách quan, tự động quét qua hàng ngàn mã cổ phiếu và chỉ để lại những cổ phiếu chất lượng tốt nhất cho công việc phân tích chuyên sâu.

        ### 2. Thế là sản phẩm này ra đời:
        Sản phẩm này được tạo ra để trả lời cho ba câu hỏi cốt lõi:
        * Đâu là những cổ phiếu chất lượng cao đáng để đầu tư (Theo Hạng QMJ)?. Câu hỏi này giải quyết 90% thời gian tìm kiếm cổ phiếu chất lượng.
        * Trong số những cổ phiếu chất lượng đó, đâu là những cổ phiếu đang được định giá hấp dẫn (theo điểm Value). Câu hỏi này giúp giảm rủi ro mua cổ phiếu chất lượng tốt nhưng lại được định giá quá cao trên thị trường.
        * Trong số những cổ phiếu chất lượng đó, đâu là những cổ phiếu đang có đà tăng trưởng mạnh mẽ (theo điểm Momentum). Câu hỏi này giúp chúng ta xác định thời điểm đúng để mua vào những mã cổ phiếu chất lượng đó.

        Để bộ lọc thực sự chất lượng, tôi áp dụng phương pháp luận từ quỹ AQR Capital Management:
        * **Quality Minus Junk (QMJ):** Nén toàn bộ mớ bòng bong báo cáo tài chính thành một điểm số Z-Score duy nhất. Nó đánh giá sức khỏe doanh nghiệp qua 3 trụ cột (Lợi nhuận, Tăng trưởng, An toàn) để gạt bỏ mấy công ty "rác" và giữ lại hàng chất lượng.
        * **Tối ưu thời gian:** Kết hợp thêm Định giá hấp dẫn (Value) và Đà tăng (Momentum), hệ thống thu hẹp danh sách 1600 mã xuống còn vài chục mã tiềm năng chỉ trong vài cú click chuột.

        Tóm lại, thời gian là thứ đắt đỏ nhất. Thay vì kiệt sức vì đi tìm data, giờ đây chúng ta có thể dành 100% năng lượng não bộ để đào sâu vào những "viên kim cương" đã được hệ thống chuẩn bị sẵn.

        > *Lưu ý: Bảng dữ liệu này là một Proof of Concept (PoC) phục vụ mục đích trình diễn năng lực xây dựng Kiến trúc Dữ liệu (Data Engineering). Đây hoàn toàn không phải là lời khuyên hay khuyến nghị đầu tư.*
        """)

def render_main_content(df: pl.DataFrame, selected_q: str, updated_time: str):
    """Hàm hiển thị bảng dữ liệu với thứ tự cột đã được sắp xếp lại"""
    st.header(f"{selected_q}", divider="gray")
    st.caption(f"⏱️ Cập nhật lần cuối: {updated_time}")

    # 1. ĐỊNH NGHĨA THỨ TỰ HIỂN THỊ (Sắp xếp lại danh sách cột)
    # Bác liệt kê các cột muốn HIỆN THEO THỨ TỰ từ trái sang phải ở đây
    display_order = [
        "qmj_rank",          # Đưa Hạng lên đầu tiên
        "ticker",            # Mã CK
        "company_name",      # Tên công ty
        "exchange",          # Sàn
        "industry_group",    # Nhóm ngành
        "sector_detail",     # Lĩnh vực
        "qmj_score",         # Điểm tổng
        "qmj_profitability", # P
        "qmj_growth",        # G
        "qmj_safety",        # S         # Kỳ báo cáo
        "current_market_cap",# Vốn hóa hiện tại
        "quarter_market_cap",# Vốn hóa chốt quý
        "value_raw_score",       # Định giá gốc (Lịch sử)
        "z_value_historical",    # Định giá chéo Z-score (Lịch sử)
        "momentum_raw_score",    # Đà tăng gốc (Lịch sử)
        "z_momentum_historical", # Đà tăng chéo Z-score (Lịch sử)
        
        # --- CỤM TIME-SERIES & CROSS-SECTIONAL (GẦN ĐÂY) ---
        "value_recent_score",    # Định giá gốc (Gần đây)
        "z_value_recent",        # Định giá chéo Z-score (Gần đây)
        "momentum_recent_score", # Đà tăng gốc (Gần đây)
        "z_momentum_recent"      # Đà tăng chéo Z-score (Gần đây)
    ]

    # 2. CHỈ CHỌN CÁC CỘT TRÊN (Tự động ẩn các cột shares, volume, kỹ thuật...)
    # Việc dùng .select ở đây sẽ loại bỏ hoàn toàn các cột bác không liệt kê ở trên
    df_display = df.select(display_order)

    # 3. CẤU HÌNH HIỂN THỊ (Làm đẹp tên cột)
    view_config = {
        "qmj_rank": st.column_config.NumberColumn(
            "Hạng (Rank)",
            width="small",
            help="Thứ tự xếp hạng của doanh nghiệp dựa trên điểm QMJ tổng hợp trong kỳ báo cáo."
        ),
        "ticker": st.column_config.TextColumn(
            "Mã Cổ Phiếu (Ticker)", 
            width="small",
            help="Mã niêm yết của cổ phiếu trên các sàn giao dịch chứng khoán."
        ),
        "company_name": st.column_config.TextColumn(
            "Tên Công Ty (Company Name)",
            width="large", 
            help="Tên đầy đủ của pháp nhân phát hành cổ phiếu."
        ),
        "exchange": st.column_config.TextColumn("Sàn (Exchange)", width="small"),
        "industry_group": st.column_config.TextColumn("Nhóm Ngành (Industry Group)", width="medium"),
        "sector_detail": st.column_config.TextColumn("Lĩnh vực (Sector)", width="medium"),
        
        "qmj_score": st.column_config.NumberColumn(
            "Điểm QMJ (QMJ Score)", 
            width="medium",
            help="Điểm chất lượng tổng hợp được chuẩn hóa từ ba nhóm nhân tố: Lợi nhuận (P), Tăng trưởng (G) và An toàn (S).",
            format="%.2f", min_value=0, max_value=1
        ),
        "qmj_profitability": st.column_config.NumberColumn(
            "Lợi nhuận (Profitability Z)", 
            width="medium",
            help="Nhân tố đo lường hiệu quả sinh lời trên tài sản và nguồn vốn (Profitability).",
            format="%.2f"
        ),
        "qmj_growth": st.column_config.NumberColumn(
            "Tăng trưởng (Growth Z)", 
            width="medium",
            help="Nhân tố đo lường tốc độ tăng trưởng của các chỉ tiêu tài chính cốt lõi trong 1 giai đoạn(Growth).",
            format="%.2f"
        ),
        "qmj_safety": st.column_config.NumberColumn(
            "An Toàn (Safety Z)", 
            width="medium",
            help="Nhân tố đo lường mức độ an toàn tài chính dựa trên đòn bẩy, biến động giá và rủi ro phá sản (Safety).",
            format="%.2f"
        ),
        
        "current_market_cap": st.column_config.NumberColumn(
            "Vốn Hoá Gần Đây (Mkt Cap - Recent)", 
            width="medium",
            help="Giá trị vốn hóa thị trường hiện tại của doanh nghiệp (tỷ đồng).",
            format="%,.0f"
        ),
        "quarter_market_cap": st.column_config.NumberColumn(
            "Vốn Hoá Chốt Quý (Mkt Cap - Quarter End)", 
            width="medium",
            help="Giá trị vốn hóa thị trường tại ngày kết thúc quý báo cáo (tỷ đồng).",
            format="%,.0f"
        ),
        
        # --- CẤU HÌNH CÁC CỘT TIME-SERIES (GỐC) ---
        "value_raw_score": st.column_config.NumberColumn(
            "Định giá Gốc Lịch sử (Value Raw - Hist)", 
            width="medium",
            help="Điểm Value gốc trong quá khứ. Dùng để phân tích Time-series: So sánh định giá của cổ phiếu với chính lịch sử của nó.",
            format="%.4f"
        ),
        "momentum_raw_score": st.column_config.NumberColumn(
            "Đà tăng Gốc Lịch sử (Mom Raw - Hist)", 
            width="medium",
            help="Điểm Momentum gốc trong quá khứ. Đo lường sức mạnh giá tự thân trong lịch sử.",
            format="%.4f"
        ),
        "value_recent_score": st.column_config.NumberColumn(
            "Định giá Gốc Gần đây (Value Raw - Recent)", 
            width="medium",
            help="Điểm Value gốc gần đây. Dùng để xem xét cổ phiếu hiện tại đang rẻ hay đắt so với mức trung bình lịch sử của nó.",
            format="%.4f"
        ),
        "momentum_recent_score": st.column_config.NumberColumn(
            "Đà tăng Gốc Gần đây (Mom Raw - Recent)", 
            width="medium",
            help="Điểm Momentum gốc gần đây. Đo lường sức mạnh giá tự thân tại thời điểm hiện tại.",
            format="%.4f"
        ),

        # --- CẤU HÌNH CÁC CỘT CROSS-SECTIONAL (Z-SCORE) ---
        "z_value_historical": st.column_config.NumberColumn(
            "Định giá Z-Score Lịch sử (Value Z - Hist)", 
            width="medium",
            help="Phân tích Cross-sectional: Cổ phiếu này rẻ hay đắt so với các cổ phiếu khác trên thị trường trong quá khứ.",
            format="%.2f"
        ),
        "z_momentum_historical": st.column_config.NumberColumn(
            "Đà tăng Z-Score Lịch sử (Mom Z - Hist)", 
            width="medium",
            help="Phân tích Cross-sectional: Đà tăng giá mạnh hay yếu so với phần còn lại của thị trường trong quá khứ.",
            format="%.2f"
        ),
        "z_value_recent": st.column_config.NumberColumn(
            "Định Giá Z-Score Gần Đây (Value Z - Recent)", 
            width="medium",
            help="Phân tích Cross-sectional: Cổ phiếu này hiện tại đang rẻ hay đắt so với các cổ phiếu khác.",
            format="%.2f"
        ),
        "z_momentum_recent": st.column_config.NumberColumn(
            "Đà tăng Z-Score Gần Đây (Mom Z - Recent)", 
            width="medium",
            help="Phân tích Cross-sectional: Cường độ dòng tiền hiện tại mạnh hay yếu so với thị trường chung.",
            format="%.2f"
        ),
    }
    # 4. VẼ BẢNG
    st.dataframe(
        df_display, 
        column_config=view_config, 
        use_container_width=True, 
        hide_index=True
    )
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
        df_filtered, selected_q, selected_top_n, selected_criteria = render_filters(df_raw)
        if selected_criteria == "Hạng QMJ":
            insight_text = f"Trong **{selected_q}**, những cổ phiếu nào lọt vào **Top {selected_top_n}** doanh nghiệp có nền tảng chất lượng nhất (theo QMJ Score)?"
        elif selected_criteria == "Đà tăng trưởng (Top Momentum)":
            insight_text = f"Trong **{selected_q}**, nếu chỉ xét trong **Top {selected_top_n}** cổ phiếu chất lượng nhất, thì những mã nào đang có **đà tăng trưởng mạnh nhất**?"
        elif selected_criteria == "Định giá (Top Value)":
            insight_text = f"Trong **{selected_q}**, nếu chỉ xét trong **Top {selected_top_n}** cổ phiếu chất lượng nhất, thì những mã nào đang có **định giá rẻ và hấp dẫn nhất**?"
        else:
            insight_text = f"Danh sách cổ phiếu theo {selected_criteria}."
        st.caption(f"**Bảng dưới đây trả lời cho câu hỏi:** *{insight_text}*")
        
        # 3. Đưa dữ liệu đã lọc lên bảng vẽ
        render_main_content(df_filtered, selected_q, updated_time)
    else:
        st.error("⚠️ Không tìm thấy file dữ liệu (data_qmj.csv)!")
    _introduction()




if __name__ == "__main__":
    main()