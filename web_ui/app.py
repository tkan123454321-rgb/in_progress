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
    df = df.with_columns(
        [
            pl.concat_str(
                [
                    pl.lit("Quý"),
                    pl.lit(" "),
                    pl.col("quarter"),
                    pl.lit(" - "),
                    pl.col("year"),
                ]
            ).alias("ui_label"),
            (pl.col("current_market_cap") / 1_000_000_000).alias("current_market_cap"),
            (pl.col("quarter_market_cap") / 1_000_000_000).alias("quarter_market_cap"),
        ]
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
    return int(df_filtered_by_quarter["qmj_rank"].max())  # type: ignore


def _get_latest_update_time(df: pl.DataFrame) -> str:
    """Lấy thời gian cập nhật mới nhất từ cột gold_updated_at"""
    latest_time = str(df["gold_updated_at"].max())
    dt_obj = datetime.strptime(latest_time[:19], "%Y-%m-%dT%H:%M:%S")
    return dt_obj.strftime("%d-%m-%y %H:%M:%S")


# ==========================================
# 2. UI COMPONENTS (CÁC THÀNH PHẦN GIAO DIỆN)
# ==========================================
def render_filters(df: pl.DataFrame):
    """Hàm này chỉ chuyên lo việc vẽ bộ lọc và trả về dữ liệu đã lọc"""
    list_q = _get_quarters_for_selectbox(df)

    col1, col2, col3 = st.columns([1, 1, 1])

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
            "Định giá (Top Value)": {"col": "z_value_recent", "desc": True},
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
            help=help_text,
        )
    with col3:
        search_ticker = (
            st.text_input("Tìm nhanh mã CK:", placeholder="VD: VNM, HPG...")
            .upper()
            .strip()
        )

    max_rank = _get_top_qmj_rank(df_filtered_by_q)
    selected_top_n = st.slider(
        "Hiển thị Top cổ phiếu (theo Hạng QMJ):",
        min_value=1,
        max_value=max_rank,
        value=max_rank,
        step=1,
    )
    sort_col = criteria_dict[selected_criteria]["col"]
    is_desc = criteria_dict[selected_criteria]["desc"]

    # Lọc lần cuối theo Rank
    if search_ticker:
        df_final = df_filtered_by_q.filter(
            pl.col("ticker").str.contains(search_ticker)
        ).sort(sort_col, descending=is_desc)
    else:
        df_final = (
            df_filtered_by_q.filter(pl.col("qmj_rank") <= selected_top_n).sort(
                sort_col, descending=is_desc
            )  # Hạng 1 lên đầu
        )

    return df_final, selected_q, selected_top_n, selected_criteria


def _introduction():
    with st.expander(" Tại sao sản phẩm này ra đời? (The 'Why' behind the Product) "):
        st.markdown("""
        ### 1. Câu chuyện của mình: khi kiến thức tài chính là không đủ.
        Đầu năm 3 đại học, sau khi nhận email báo đỗ CFA Level 1 với điểm số 1755/1900, mình từng nghĩ: Mình sẽ chọn một mã cổ phiếu, áp dụng đống công thức định giá xịn vừa học được, viết một cái report thật chất lượng 20-30 trang PDF rồi tự tin gửi CV ứng tuyển vào khối phân tích của các công ty chứng khoán hoặc các quỹ đầu tư thì bao pass cv và vào thẳng phỏng vấn.

        Nhưng hiện thực nó không vận hành như vậy. Bắt tay vào làm thì mới thấy khó khăn xuất hiện dần dần.
        Việc chọn ra 1 mã cổ phiếu bất kỳ để phân tích, Lôi báo cáo tài chính ra để xuất excel xong bóc tách, xong lại "kéo Excel" để tính P/E, P/B, ROE... cho một công ty sẽ mất vô cùng nhiều thời gian. Điều tệ nhất là hì hục làm mới nhận ra mã đó vốn dĩ không có tiềm năng dài hạn, nền tảng của nó không đủ tốt thì lúc đó chắc chắn công sức như muối bỏ bể. Nó giống như việc đãi cát tìm vàng trong danh sách 1,600 mã cổ phiếu trên sàn chứng khoán Việt Nam.

        Sự bế tắc đó làm mình nhận ra: nếu như đi theo con đường truyền thống bằng cách chọn từng mã phân tích thủ công thì sẽ vô cùng may rủi và tốn thời gian.
        Do đó, thay vì cố chấp làm một chuyên viên phân tích tài chính chọn mã bằng sức người, mình chọn lùi lại 1 bước để nhìn toàn cảnh. Mình đã chọn trở thành một data engineer để xây một hệ thống pipeline dữ liệu sàng lọc tự động, khách quan, tự động quét qua hàng ngàn mã cổ phiếu và chỉ để lại những cổ phiếu chất lượng tốt nhất cho công việc phân tích chuyên sâu.

        ### 2. Thế là sản phẩm này ra đời:
        Sản phẩm này được tạo ra để trả lời cho ba câu hỏi cốt lõi:
        * Đâu là những cổ phiếu chất lượng cao đáng để đầu tư (Theo Hạng QMJ)? -> Câu hỏi này giải quyết 90% thời gian tìm kiếm cổ phiếu chất lượng.
        * Trong số những cổ phiếu chất lượng đó, đâu là những cổ phiếu đang được định giá hấp dẫn (theo điểm Value)? -> Câu hỏi này giúp giảm rủi ro mua cổ phiếu chất lượng tốt nhưng lại được định giá quá cao trên thị trường.
        * Trong số những cổ phiếu chất lượng đó, đâu là những cổ phiếu đang có đà tăng trưởng mạnh mẽ (theo điểm Momentum)? -> Câu hỏi này giúp chúng ta xác định thời điểm đúng để mua vào những mã cổ phiếu chất lượng đó.

        ### 3. Cốt lõi của sản phẩm này: điểm QMJ (Quality Minus Junk) và sự kết hợp giữa điểm Value & Momentum
        Để sản phẩm này đưa ra các tín hiệu chính xác và khách quan, lõi thuật toán (Data Pipeline) được xây dựng dựa trên các nghiên cứu định lượng từ công ty quản lý đầu tư toàn cầu AQR Capital Management:

        * [QMJ (Quality Minus Junk)](https://link.springer.com/article/10.1007/s11142-018-9470-2): Lượng hóa chất lượng doanh nghiệp bằng toán học để loại bỏ hoàn toàn cảm tính. Thay vì dùng một chỉ số đơn lẻ dễ bị thao túng, hệ thống thu thập hàng chục chỉ số tài chính thô, xếp hạng (Rank) và chuẩn hóa chúng thành điểm Z-Score. Bằng cách lấy trung bình cộng, thuật toán khử nhiễu (reduce noise) và đánh giá sức khỏe doanh nghiệp qua 3 trụ cột:
            * Sinh lời (Profitability): Đo lường năng lực tạo ra lợi nhuận bền vững thông qua 6 chỉ số đa chiều (ROE, ROA, Dòng tiền, Biên gộp...).
            * Tăng trưởng (Growth): Đánh giá tốc độ gia tăng của lợi nhuận cốt lõi trong một chu kỳ dài hạn.
            * An toàn (Safety): Đo lường khả năng sinh tồn dựa trên rủi ro phá sản, đòn bẩy nợ và mức độ biến động giá cổ phiếu.
        * [Value And Momentum Everywhere](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2174501): Sau khi có những mã có điểm quality cao (QMJ Score), hệ thống tiếp tục xác định cổ phiếu đang được định giá hấp dẫn (Value) và cổ phiếu nào đang có đà tăng trưởng (Momentum) để từ đó đưa ra những quyết định đầu tư tốt hơn.

        > *Lưu ý: Bảng dữ liệu này là một dự án cá nhân (Portfolio Project) để chứng minh khả năng xử lý và xây dựng kiến trúc dữ liệu (Data Engineering). Các số liệu và bảng xếp hạng ở đây hoàn toàn không phải là lời khuyên hay khuyến nghị đầu tư.*
        """)


def _faq():
    with st.expander(" 💡 FAQ (Giải đáp thắc mắc về dữ liệu) "):
        st.markdown(
            "### Tại sao chỉ có số ít mã cổ phiếu được chấm điểm QMJ theo từng quý?"
        )
        st.write("""
        Để đảm bảo tính chính xác và thực chiến của bảng xếp hạng, mình đã thực hiện một quy trình sàng lọc qua 4 lớp chính:
        1.  Lọc quy mô và thanh khoản: Bắt đầu từ 1.548 mã trên sàn, mình chỉ giữ lại những doanh nghiệp có vốn hóa (Market Cap) trên 500 tỷ VNĐ và khối lượng giao dịch trung bình 3 tháng trên 10.000 cổ phiếu/phiên. Điều này giúp loại bỏ các cổ phiếu không có thanh khoản hoặc quá bé về mặt quy mô.
        2.  Loại bỏ ngành đặc thù: Hệ thống tự động loại khỏi danh sách các cổ phiếu nhóm Ngân hàng, Chứng khoán và Bảo hiểm do cấu trúc BCTC và mô hình kinh doanh khác biệt hoàn toàn, không phù hợp để dùng chung thang đo QMJ với các ngành sản xuất, dịch vụ.
        3.  Kiểm soát tính toàn vẹn dữ liệu (Data Integrity): Do hệ thống được xây từ nguồn dữ liệu công khai, sai sót là không thể tránh khỏi. Do đó hệ thống tự động loại bỏ các BCTC của các quý bị lỗi, bị thiếu dữ liệu quan trọng hoặc có sự bất thường rõ ràng. Việc này đảm bảo những doanh nghiệp có dữ liệu đầy đủ và chính xác mới được chấm điểm QMJ, từ đó nâng cao độ tin cậy của bảng xếp hạng.
        4.  Hạn chế về phương pháp Lưu chuyển tiền tệ: Các doanh nghiệp dùng phương pháp Lưu chuyển tiền tệ trực tiếp không thể bóc tách chi tiết mục Khấu hao (Depreciation) trong các nguồn dữ liệu miễn phí. Vì đây là biến số trọng yếu trong QMJ, hệ thống chấp nhận loại bỏ những mã này để đảm bảo sự công bằng và đầy đủ cho điểm số cuối cùng.
        **Giải pháp mở rộng:** sản phẩm này hoàn toàn sẵn sàng cho việc mở rộng quy mô. Nếu được tích hợp với các nguồn dữ liệu chuyên sâu (Paid Data) đầy đủ và chính xác hơn, hệ thống sẽ tự động tăng số lượng mã được xếp hạng lên toàn bộ thị trường mà vẫn giữ vững các tiêu chuẩn khắt khe về chất lượng dữ liệu.
        """)
        st.markdown("### Tại sao bảng dữ liệu có cả điểm Raw Score và Z-Score?")
        st.write("""
        Bảng dữ liệu cung cấp hai góc nhìn bổ trợ nhau giúp đánh giá cổ phiếu toàn diện:

        1. Raw Score (Điểm Gốc - So sánh tự thân): Dùng để biết cổ phiếu đang tốt hơn hay tệ đi so với chính nó trong quá khứ. Ví dụ: Điểm Value gần đây thấp hơn lịch sử nghĩa là cổ phiếu đang rẻ hơn chính mức trước đây của nó.

        2. Z-Score (Thứ hạng - So sánh thị trường): Dùng để so sánh cổ phiếu so với những mã còn lại. Vì mỗi chỉ số (P/E, lợi nhuận, đà tăng) có đơn vị khác nhau, Z-Score giúp đưa tất cả về cùng một hệ quy chiếu chuẩn để so sánh công bằng.
        """)


def _technical_adjustments():
    with st.expander(" Hiệu chỉnh Kỹ thuật  "):
        st.markdown("""
        Để thuật toán vận hành thực tế và khách quan nhất tại thị trường Việt Nam, sản phẩm đã được thay đổi 1 số điểm so với bài báo gốc của AQR:
        * Chu kỳ Tăng trưởng (Growth Window): Thay vì dùng 20 quý (5 năm) lịch sử làm mốc cửa sổ tham chiếu trong mục tính thành phần tăng trưởng (growth), mình đã rút ngắn xuống còn 16 quý (4 năm).
            * Lý do: Hệ thống ưu tiên tính toàn vẹn của dữ liệu. Tại Việt Nam, yêu cầu dữ liệu sạch liên tiếp trong 20 quý sẽ khiến số lượng mã bị loại bỏ rất lớn do lỗi dữ liệu quá khứ từ nguồn cung cấp. 16 quý là con số đủ ổn để cân bằng giữa việc đo lường nội lực dài hạn và duy trì số lượng mẫu đủ lớn để so sánh.
        * Sử dụng báo cáo tài chính theo TTM (Trailing Twelve Months) thay vì Annual.
            * lý do: Thay vì đợi báo cáo năm tài chính (thường có độ trễ lớn), mình sử dụng dữ liệu trượt 4 quý gần nhất. Điều này giúp rút ngắn tối đa độ trễ dữ liệu và đưa ra các tín hiệu sớm hơn.
        """)


def render_main_content(df: pl.DataFrame, selected_q: str, updated_time: str):
    """Hàm hiển thị bảng dữ liệu với thứ tự cột đã được sắp xếp lại"""
    st.header(f"{selected_q}", divider="gray")
    st.caption(f"⏱️ Cập nhật lần cuối: {updated_time}")

    # 1. ĐỊNH NGHĨA THỨ TỰ HIỂN THỊ (Sắp xếp lại danh sách cột)
    # Bác liệt kê các cột muốn HIỆN THEO THỨ TỰ từ trái sang phải ở đây
    display_order = [
        "qmj_rank",  # Đưa Hạng lên đầu tiên
        "ticker",  # Mã CK
        "company_name",  # Tên công ty
        "exchange",  # Sàn
        "industry_group",  # Nhóm ngành
        "sector_detail",  # Lĩnh vực
        "qmj_score",  # Điểm tổng
        "qmj_profitability",  # P
        "qmj_growth",  # G
        "qmj_safety",  # S         # Kỳ báo cáo
        "current_market_cap",  # Vốn hóa hiện tại
        "quarter_market_cap",  # Vốn hóa chốt quý
        "value_recent_score",  # Định giá gốc (Gần đây)
        "momentum_recent_score",  # Đà tăng gốc (Gần đây)
    ]

    # 2. CHỈ CHỌN CÁC CỘT TRÊN (Tự động ẩn các cột shares, volume, kỹ thuật...)
    # Việc dùng .select ở đây sẽ loại bỏ hoàn toàn các cột bác không liệt kê ở trên
    df_display = df.select(display_order)

    # 3. CẤU HÌNH HIỂN THỊ (Làm đẹp tên cột)
    view_config = {
        "qmj_rank": st.column_config.NumberColumn(
            "Hạng (Rank)",
            width="small",
            help="Thứ tự xếp hạng của doanh nghiệp dựa trên điểm QMJ tổng hợp trong kỳ báo cáo.",
        ),
        "ticker": st.column_config.TextColumn(
            "Mã Cổ Phiếu (Ticker)",
            width="small",
            help="Mã niêm yết của cổ phiếu trên các sàn giao dịch chứng khoán.",
        ),
        "company_name": st.column_config.TextColumn(
            "Tên Công Ty (Company Name)",
            width="large",
            help="Tên đầy đủ của pháp nhân phát hành cổ phiếu.",
        ),
        "exchange": st.column_config.TextColumn("Sàn (Exchange)", width="small"),
        "industry_group": st.column_config.TextColumn(
            "Nhóm Ngành (Industry Group)", width="medium"
        ),
        "sector_detail": st.column_config.TextColumn(
            "Lĩnh vực (Sector)", width="medium"
        ),
        "qmj_score": st.column_config.NumberColumn(
            "Điểm QMJ (QMJ Score)",
            width="medium",
            help="Điểm chất lượng tổng hợp được chuẩn hóa từ ba nhóm nhân tố: Lợi nhuận (P), Tăng trưởng (G) và An toàn (S).",
            format="%.2f",
            min_value=0,
            max_value=1,
        ),
        "qmj_profitability": st.column_config.NumberColumn(
            "Lợi nhuận (Profitability Z)",
            width="medium",
            help="Nhân tố đo lường hiệu quả sinh lời trên tài sản và nguồn vốn (Profitability).",
            format="%.2f",
        ),
        "qmj_growth": st.column_config.NumberColumn(
            "Tăng trưởng (Growth Z)",
            width="medium",
            help="Nhân tố đo lường tốc độ tăng trưởng của các chỉ tiêu tài chính cốt lõi trong 1 giai đoạn(Growth).",
            format="%.2f",
        ),
        "qmj_safety": st.column_config.NumberColumn(
            "An Toàn (Safety Z)",
            width="medium",
            help="Nhân tố đo lường mức độ an toàn tài chính dựa trên đòn bẩy, biến động giá và rủi ro phá sản (Safety).",
            format="%.2f",
        ),
        "current_market_cap": st.column_config.NumberColumn(
            "Vốn Hoá Gần Đây (Mkt Cap - Recent)",
            width="medium",
            help="Giá trị vốn hóa thị trường hiện tại của doanh nghiệp (tỷ đồng).",
            format="%,.0f",
        ),
        "quarter_market_cap": st.column_config.NumberColumn(
            "Vốn Hoá Chốt Quý (Mkt Cap - Quarter End)",
            width="medium",
            help="Giá trị vốn hóa thị trường tại ngày kết thúc quý báo cáo (tỷ đồng).",
            format="%,.0f",
        ),
        # --- CẤU HÌNH CÁC CỘT TIME-SERIES (GỐC) ---
        "value_raw_score": st.column_config.NumberColumn(
            "Định giá Gốc Lịch sử (Value Raw - Hist)",
            width="medium",
            help="dùng cho phân tích Time-series: diểm value gốc trong quá khứ, so sánh định giá của cổ phiếu với chính lịch sử của nó.",
            format="%.4f",
        ),
        "momentum_raw_score": st.column_config.NumberColumn(
            "Đà tăng Gốc Lịch sử (Mom Raw - Hist)",
            width="medium",
            help="dùng cho phân tích Time-series: Điểm Momentum gốc trong quá khứ, đo lường sức mạnh giá tự thân trong lịch sử.",
            format="%.4f",
        ),
        "value_recent_score": st.column_config.NumberColumn(
            "Định giá Gốc Gần đây (Value Raw - Recent)",
            width="medium",
            help="dùng cho phân tích Time-series: Điểm Value gốc gần đây, dùng để xem xét cổ phiếu hiện tại đang rẻ hay đắt so với mức trung bình lịch sử của nó.",
            format="%.4f",
        ),
        "momentum_recent_score": st.column_config.NumberColumn(
            "Đà tăng Gốc Gần đây (Mom Raw - Recent)",
            width="medium",
            help="dùng cho phân tích Time-series: Điểm Momentum gốc gần đây, đo lường sức mạnh giá tự thân tại thời điểm hiện tại.",
            format="%.4f",
        ),
        # --- CẤU HÌNH CÁC CỘT CROSS-SECTIONAL (Z-SCORE) ---
        "z_value_historical": st.column_config.NumberColumn(
            "Định giá Z-Score Lịch sử (Value Z - Hist)",
            width="medium",
            help="dùng cho phân tích Cross-sectional: Cổ phiếu này quá khứ rẻ hay đắt so với các cổ phiếu khác.",
            format="%.2f",
        ),
        "z_momentum_historical": st.column_config.NumberColumn(
            "Đà tăng Z-Score Lịch sử (Mom Z - Hist)",
            width="medium",
            help="dùng cho phân tích Cross-sectional: Đà tăng giá mạnh hay yếu so với thị trường trong quá khứ.",
            format="%.2f",
        ),
        "z_value_recent": st.column_config.NumberColumn(
            "Định Giá Z-Score Gần Đây (Value Z - Recent)",
            width="medium",
            help="dùng cho phân tích Cross-sectional: Cổ phiếu này hiện tại đang rẻ hay đắt so với các cổ phiếu khác.",
            format="%.2f",
        ),
        "z_momentum_recent": st.column_config.NumberColumn(
            "Đà tăng Z-Score Gần Đây (Mom Z - Recent)",
            width="medium",
            help="dùng cho phân tích Cross-sectional: đà tăng giá hiện tại mạnh hay yếu so với thị trường chung.",
            format="%.2f",
        ),
    }
    # 4. VẼ BẢNG
    st.dataframe(
        df_display, column_config=view_config, use_container_width=True, hide_index=True
    )


# ==========================================
# 3. ORCHESTRATOR (NHẠC TRƯỞNG ĐIỀU PHỐI)
# ==========================================
def main():
    st.set_page_config(page_title="QMJ Dashboard", layout="wide")
    st.title(
        "Demo Dữ liệu QMJ - 1 số cồ phiếu chưa phát hành dữ liệu quý mới nhất cho q1 2026, hãy chọn q4/2025"
    )
    st.header("Bảng danh sách dữ liệu cổ phiếu chấm điểm theo QMJ", divider="gray")

    # 1. Lấy dữ liệu gốc
    df_raw = transform_data()

    if df_raw is not None:
        # 2. Đưa dữ liệu qua bộ lọc (Lấy về dữ liệu đã lọc và quý đang chọn)
        updated_time = _get_latest_update_time(df_raw)
        df_filtered, selected_q, selected_top_n, selected_criteria = render_filters(
            df_raw
        )
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
    _faq()
    _technical_adjustments()
    st.markdown("---")
    st.caption("""
    **⚠️ TUYÊN BỐ MIỄN TRỪ TRÁCH NHIỆM (DISCLAIMER):** Đây là sản phẩm thực tế thuộc dự án cá nhân nhằm khẳng định năng lực chuyên môn trong lĩnh vực Data Engineering. Mọi dữ liệu, tín hiệu và thông tin trên hệ thống chỉ phục vụ mục đích tham khảo và CHẮN CHẮN có khả năng sai sót dữ liệu. Tác giả không cung cấp dịch vụ tư vấn tài chính và không chịu trách nhiệm cho bất kỳ quyết định mua bán hay tổn thất tài chính nào.
    """)


if __name__ == "__main__":
    main()
