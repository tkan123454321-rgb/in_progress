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
    with st.expander("📌 Về dự án này: Từ câu chuyện cá nhân đến Tư duy Hệ thống"):
        
        st.markdown("""
        ### 1. Câu chuyện khởi nguồn: Khi công thức tài chính là chưa đủ
        Đầu năm 3 đại học, khi cầm trên tay kết quả pass CFA Level 1 với số điểm 1755/1900, tôi từng nghĩ mình đã nắm được "chìa khóa" vững chắc để bước vào các quỹ đầu tư hay khối phân tích của một công ty chứng khoán. Kế hoạch lúc đó của tôi rất đơn giản (và có phần lý tưởng hóa): Chọn một mã cổ phiếu bất kỳ, lôi hết giáo trình và công thức định giá ra để viết một bản báo cáo phân tích chuyên sâu thật hoành tráng, đính kèm vào CV và nộp đi.

        Nhưng khi thực sự bắt tay vào làm, tôi nhanh chóng đâm sầm vào thực tế.
        Việc bóc tách báo cáo tài chính, tính toán từng chỉ số rời rạc (như P/E, P/B, ROE...) cho *một* mã cổ phiếu ngốn của tôi quá nhiều thời gian. Điều tồi tệ nhất là: Sau nhiều ngày hì hục phân tích, tôi ngậm ngùi nhận ra doanh nghiệp đó vốn dĩ không có tiềm năng dài hạn. Thế là mọi công sức đổ sông đổ biển. Tôi lại cặm cụi chọn một mã khác, lặp lại quy trình vắt kiệt sức lực đó, và cầu may rằng lần này mình sẽ chọn đúng.

        Tôi tự hỏi: *"Chẳng lẽ công việc của một chuyên viên phân tích tài chính là cứ phải thử-và-sai thủ công một cách tuyệt vọng giữa biển 1,600+ mã chứng khoán trên cả 3 sàn như vậy sao?"*

        ### 2. Sự chuyển dịch tư duy: Lùi lại để nhìn toàn cảnh
        Sự bế tắc đó đã buộc tôi phải lùi lại một bước. Tôi nhận ra mình đang làm việc ngược quy trình. Trước khi dùng kính lúp để soi thật kỹ một cái cây, mình phải dùng radar để quét xem khu rừng nào đáng để bước vào.

        Để giải quyết bài toán đó, tôi không cần thêm một công thức định giá phức tạp nào nữa. Thứ tôi cần là một **Hệ thống Sàng lọc (Screening System)** khách quan, tự động và dựa trên dữ liệu lớn (Data-driven). Một hệ thống đủ thông minh để tìm ra những cổ phiếu thực sự xứng đáng để mình đầu tư quỹ thời gian hạn hẹp vào phân tích chuyên sâu.

        Đó là lý do dự án này ra đời.

        ### 3. Giải pháp: Data Pipeline kết hợp Phương pháp luận AQR
        Thay vì để người dùng "bơi" trong dữ liệu thô, sản phẩm này được thiết kế với tư duy **"Data as a Product"**. Hệ thống phía sau tự động thu thập, chuẩn hóa và xử lý dữ liệu của toàn thị trường.

        **Cốt lõi của bộ lọc dựa trên nghiên cứu học thuật từ quỹ AQR Capital Management:**
        * **Quality Minus Junk (QMJ):** Tổng hợp hàng loạt báo cáo tài chính phức tạp thành một điểm số Z-Score duy nhất. Nó đánh giá toàn diện dựa trên 3 trụ cột: Lợi nhuận (Profitability), Tăng trưởng (Growth) và An toàn (Safety) để loại trừ triệt để những cổ phiếu "Rác".
        * **Tối ưu hóa thời gian:** Bằng cách kết hợp QMJ với các nhân tố Định giá (Value) và Đà tăng trưởng (Momentum), hệ thống thu hẹp góc nhìn từ vĩ mô xuống vi mô chỉ trong vài cú click.

        Thay vì mất hàng giờ tìm kiếm lan man, giờ đây nhà đầu tư và chuyên viên phân tích có thể dành toàn bộ trí lực để đào sâu vào một nhóm nhỏ các "cổ phiếu kim cương" đã được hệ thống bảo chứng về nền tảng cơ bản.

        > *Lưu ý: Bảng dữ liệu này phục vụ mục đích trình diễn năng lực xây dựng Kiến trúc Dữ liệu (Data Engineering) và tự động hóa hệ thống. Đây không phải là lời khuyên hay khuyến nghị đầu tư.*
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
        "z_value_historical",
        "z_momentum_historical",
        "z_value_recent",
        "z_momentum_recent"
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
            width="large", # Tên công ty siêu dài nên để large
            help="Tên đầy đủ của pháp nhân phát hành cổ phiếu."
        ),
        
        # Mấy cột này phải bọc vào TextColumn thì mới thêm width được
        "exchange": st.column_config.TextColumn("Sàn (Exchange)", width="small"),
        "industry_group": st.column_config.TextColumn("Nhóm Ngành (Industry Group)", width="medium"),
        "sector_detail": st.column_config.TextColumn("Lĩnh vực (Sector)", width="medium"),
        "ui_label": st.column_config.TextColumn("Kỳ báo cáo (Period)", width="medium"),
        
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
        
        "z_value_historical": st.column_config.NumberColumn(
            "Định giá Lịch Sử (Value Z-Historical)", 
            width="medium",
            help="Điểm Z-Score trung bình của các chỉ số định giá trong các giai đoạn quá khứ.",
            format="%.2f"
        ),
        "z_momentum_historical": st.column_config.NumberColumn(
            "Đà tăng Lịch Sử (Momentum Z-Historical)", 
            width="medium",
            help="Điểm Z-Score trung bình của đà tăng trưởng giá trong các giai đoạn quá khứ.",
            format="%.2f"
        ),
        "z_value_recent": st.column_config.NumberColumn(
            "Định Giá Gần Đây (Value Z-Recent)", 
            width="medium",
            help="Điểm Z-Score phản ánh mức độ định giá hiện tại so với tập hợp dữ liệu so sánh.",
            format="%.2f"
        ),
        "z_momentum_recent": st.column_config.NumberColumn(
            "Đà tăng Gần Đây (Momentum Z-Recent)", 
            width="medium",
            help="Điểm Z-Score phản ánh cường độ đà tăng trưởng giá hiện tại (Price Momentum).",
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