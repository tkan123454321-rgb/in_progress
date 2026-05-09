import streamlit as st
import polars as pl
from pathlib import Path
from datetime import datetime


# ==========================================
# 1. DATA PROCESSING
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
# 2. UI COMPONENTS
# ==========================================
def render_filters(df: pl.DataFrame):
    """Hàm này chỉ chuyên lo việc vẽ bộ lọc và trả về dữ liệu đã lọc"""
    list_q = _get_quarters_for_selectbox(df)

    col1, col2, col3 = st.columns([1, 1, 1])
    # quarter column selection
    with col1:
        selected_q = st.selectbox("Chọn Kỳ Báo Cáo:", options=list_q)
        df_filtered_by_q = df.filter(pl.col("ui_label") == selected_q)
    # criteria selection
    with col2:
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

        selected_criteria = st.selectbox(
            "Chọn Tiêu chí Xếp hạng:",
            options=list(criteria_dict.keys()),
            help=help_text,
        )
    # 3. Search box for ticker
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

    if search_ticker:
        df_final = df_filtered_by_q.filter(
            pl.col("ticker").str.contains(search_ticker)
        ).sort(sort_col, descending=is_desc)
    else:
        df_final = df_filtered_by_q.filter(pl.col("qmj_rank") <= selected_top_n).sort(
            sort_col, descending=is_desc
        )

    return df_final, selected_q, selected_top_n, selected_criteria


# ==========================================
# CONTENT
# =========================================
def _introduction():
    with st.expander(" Tại sao sản phẩm này ra đời? "):
        st.markdown("""
        ### 1. Câu chuyện của mình: khi kiến thức tài chính là không đủ.
        Đầu năm 3 đại học, sau khi nhận email báo đỗ CFA Level 1 với điểm số 1755/1900, mình từng nghĩ: Mình sẽ chọn một mã cổ phiếu, áp dụng đống công thức định giá xịn vừa học được, viết một cái report thật chất lượng 20-30 trang PDF rồi tự tin gửi CV ứng tuyển vào khối phân tích của các công ty chứng khoán hoặc các quỹ đầu tư thì bao pass cv và vào thẳng phỏng vấn.

        Nhưng hiện thực nó không vận hành như vậy. Bắt tay vào làm thì mới thấy khó khăn xuất hiện dần dần.
        Việc chọn ra 1 mã cổ phiếu bất kỳ để phân tích, Lôi báo cáo tài chính ra để xuất excel xong bóc tách, xong lại "kéo Excel" để tính P/E, P/B, ROE... cho một công ty sẽ mất vô cùng nhiều thời gian. Điều tệ nhất là hì hục làm mới nhận ra mã đó vốn dĩ không có tiềm năng dài hạn, nền tảng của nó không đủ tốt thì lúc đó chắc chắn công sức như muối bỏ bể. Nó giống như việc đãi cát tìm vàng trong danh sách 1,600 mã cổ phiếu trên sàn chứng khoán Việt Nam.

        Sự bế tắc đó làm mình nhận ra: nếu như đi theo con đường truyền thống bằng cách chọn từng mã phân tích thủ công thì sẽ vô cùng may rủi và tốn thời gian.
        Do đó, thay vì cố chấp làm một chuyên viên phân tích tài chính chọn mã bằng sức người, mình chọn lùi lại 1 bước để nhìn toàn cảnh. Mình đã chọn trở thành một data engineer để xây một hệ thống pipeline dữ liệu sàng lọc tự động, khách quan, tự động quét qua hàng ngàn mã cổ phiếu và chỉ để lại những cổ phiếu chất lượng tốt nhất cho công việc phân tích chuyên sâu.

        ### 2. Thế là sản phẩm này ra đời:
        Để đảm bảo sản phẩm này đưa ra các tín hiệu chính xác và khách quan dựa trên các công bố được kiểm chứng và xác nhận, lõi thuật toán và các chỉ số được lựa chọn và xây dựng dựa trên các nghiên cứu định lượng từ công ty quản lý đầu tư toàn cầu AQR Capital Management qua 2 bài báo chính:
        * "[Quality Minus Junk](https://link.springer.com/article/10.1007/s11142-018-9470-2)" (Xuất bản công khai 05-11-2018): Nghiên cứu này giới thiệu thuật toán chấm điểm QMJ (Quality Minus Junk) để đánh giá chất lượng doanh nghiệp dựa trên 3 nhóm nhân tố: Lợi nhuận (Profitability), Tăng trưởng (Growth) và An toàn (Safety). Điểm QMJ tổng hợp giúp xếp hạng các cổ phiếu theo chất lượng nền tảng tài chính.
        * "[Value and Momentum Everywhere](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2174501)" (Xuất bản công khai 14-11-2012): Nghiên cứu này trình bày cách xây dựng các chỉ số Định giá (Value) và Đà tăng trưởng (Momentum) để đo lường mức độ hấp dẫn về giá và sức mạnh xu hướng của cổ phiếu trên nhiều thị trường và lớp tài sản khác nhau.

        Bộ lọc này sinh ra để xử lý triệt để cái đau đầu của dân đầu tư: **Mất quá nhiều thời gian**.

        Về bản chất, nó là một cái phễu lọc định lượng cơ bản tự động (quantamental screening), giúp chúng ta thu hẹp cả thị trường lại xuống 1 danh sách cổ phiếu chất lượng tốt bằng cách trả lời 3 câu hỏi cốt lõi trước khi xuống tiền:

        #### 1. Công ty này làm ăn được ko không? (Lọc hàng chất lượng bằng điểm QMJ)
        Thay vì bỏ ra cả tuần lật từng trang Báo cáo tài chính, soi từng con số để mò mẫm ra được vài công ty làm ăn đàng hoàng giữa hơn 1.600 mã trên sàn, điểm QMJ là cái máy quét chính. Nó gạt bỏ mọi yếu tố cảm xúc và chắt lọc thành 1 danh sách chỉ gồm những doanh nghiệp khỏe mạnh nhất, lợi nhuận đều, ít rủi ro nợ nần. Đây là lớp phòng thủ an toàn đầu tiên.

        #### 2. Hàng tốt đấy, nhưng cổ phiếu đó có bị định giá quá cao không? (Tránh mua giá quá cao bằng điểm Value )
        Danh sách công ty chất lượng thì có rồi, nhưng giá cổ phiếu đang quá cao so với giá trị nội tại cũng không ổn. Nên từ danh sách cổ phiếu chất lượng đã qua vòng QMJ, hệ thống dùng điểm Value để quét tiếp xem mã nào đang bị thị trường định giá thấp. Nó giúp chúng ta tìm được hàng ngon nhưng vẫn đang ở mức giá rẻ.

        #### 3. Ngon, bổ, rẻ rồi, múc luôn hay đợi? (Đo sóng dòng tiền bằng  điểm Momentum)
        Mua được cổ phiếu tốt, định giá lại đang quá rẻ. Nhưng nếu ôm xong cứ để đấy 1-2 năm giá nó chẳng chịu nhúc nhích vì dòng tiền ngoài thị trường đang đổ ở chỗ khác. Ôm hàng, chôn vốn cực kỳ ức chế. Lúc này ta sẽ dùng Điểm Momentum là lớp màng lọc cuối cùng, nó chỉ mặt đặt tên những cổ phiếu (vốn dĩ đã tốt và rẻ) đang bắt đầu hút tiền mạnh bây giờ.

        > *Lưu ý: Bảng dữ liệu này là một dự án cá nhân (Portfolio Project) để chứng minh khả năng xử lý và xây dựng kiến trúc dữ liệu (Data Engineering). Các số liệu và bảng xếp hạng ở đây hoàn toàn không phải là lời khuyên hay khuyến nghị đầu tư.*
        """)
        st.image(
            str(Path(__file__).parent / "assets" / "solution.png"),
            use_container_width=True,
        )
        st.caption(
            "*(Luồng xử lý tự động từ khâu thu thập dữ liệu thô đến danh mục đầu tư hoàn chỉnh)*"
        )
        st.markdown("""
        **Nhìn vào sơ đồ trên, chúng ta có thể thấy rõ toàn bộ quy trình sàng lọc của hệ thống:**
        * **Tầng 1 - Lọc thô (primary filter):** Hút toàn bộ dữ liệu thị trường từ 3 sàn (HOSE, HNX, UPCOM). Ngay lập tức, hệ thống tự động gạt bỏ những mã thanh khoản thấp (không ai mua bán) và vốn hóa quá bé (cổ phiếu Penny).
        * **Tầng 2 - Phễu lọc lõi (quantamental screening):** Áp dụng thuật toán chấm điểm Chất lượng (QMJ Score). phễu sẽ chỉ dữ liệu những doanh nghiệp có tiềm năng tăng trưởng bền vững.
        * **Tầng 3 - Phân nhóm (Classified Watchlists):** Các cổ phiếu chất lượng vượt qua tầng 2 sẽ được máy móc tự động phân loại vào các rổ theo dõi dựa trên Định giá (Value) và Đà tăng (Momentum) hoặc kết hợp cả 2 rổ chỉ số này.
        * **Tầng 4 - Sự tham gia của con người (Human Oversight & Portfolio Construction):** Máy móc đã làm xong 90% công việc tay chân. Giờ đây, các Chuyên viên phân tích (Financial Analysts) sẽ tham gia phân tích chuyên sâu để lọc nhiễu, phân tích tiềm năng tăng trưởng,.. Cuối cùng, các nhà quản lý danh mục (Portfolio Manager) tập trung vào xây dựng danh mục đầu tư tối ưu dựa trên các tín hiệu đã được sàng lọc kỹ lưỡng từ hệ thống.
        > *Lưu ý: Bảng dữ liệu này là một dự án cá nhân (Portfolio Project) để chứng minh khả năng xử lý và xây dựng kiến trúc dữ liệu (Data Engineering). Các số liệu và bảng xếp hạng ở đây hoàn toàn không phải là lời khuyên hay khuyến nghị đầu tư.*
                    """)


def _faq():
    with st.expander(" FAQ (Giải đáp thắc mắc về dữ liệu) "):
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
    with st.expander("Hiệu chỉnh Kỹ thuật"):
        st.markdown("""
        Để thuật toán vận hành thực tế và khách quan nhất tại thị trường Việt Nam, sản phẩm đã được thay đổi 1 số điểm so với bài báo gốc của AQR:
        * Chu kỳ Tăng trưởng (Growth Window): Thay vì dùng 20 quý (5 năm) lịch sử làm mốc cửa sổ tham chiếu trong mục tính thành phần tăng trưởng (growth), mình đã rút ngắn xuống còn 16 quý (4 năm).
            * Lý do: Hệ thống ưu tiên tính toàn vẹn của dữ liệu. Tại Việt Nam, yêu cầu dữ liệu sạch liên tiếp trong 20 quý sẽ khiến số lượng mã bị loại bỏ rất lớn do lỗi dữ liệu quá khứ từ nguồn cung cấp. 16 quý là con số đủ ổn để cân bằng giữa việc đo lường nội lực dài hạn và duy trì số lượng mẫu đủ lớn để so sánh.
        * Sử dụng báo cáo tài chính theo TTM (Trailing Twelve Months) thay vì Annual.
            * lý do: Thay vì đợi báo cáo năm tài chính (thường có độ trễ lớn), mình sử dụng dữ liệu trượt 4 quý gần nhất. Điều này giúp rút ngắn tối đa độ trễ dữ liệu và đưa ra các tín hiệu sớm hơn.
        """)


def _limitations():
    with st.expander(" Giới hạn của hệ thống: Yếu tố không thể thay thế "):
        st.markdown("""
        Dù nền tảng này tự động hóa khâu thu thập và sàng lọc dữ liệu, nó sinh ra là để **hỗ trợ ra quyết định**, chứ KHÔNG THỂ thay thế quyết định và kiến thức chuyên môn của con người:
        * **Sai số dữ liệu:** Máy móc hoàn toàn phụ thuộc vào nguồn dữ liệu thô. Thỉnh thoảng các báo cáo tài chính vẫn có độ trễ, sai sót hoặc thiếu hụt số liệu. Danh sách hệ thống lọc ra chỉ là bước khởi đầu, không phải là tín hiệu chắc chắn để nhắm mắt mua theo.
        * **Phân tích cơ bản chuyên sâu:** Hệ thống tính toán các con số trong quá khứ và hiện tại cực kỳ hoàn hảo, nhưng nó không thể dự đoán tương lai. Chúng ta vẫn bắt buộc phải cần các Chuyên viên Phân tích (Financial Analysts) để mổ xẻ mô hình kinh doanh thực tế, lợi thế cạnh tranh, chất lượng ban lãnh đạo và tiềm năng tương lai thực sự của doanh nghiệp.
        * **Quản trị Danh mục Chiến lược:** Lắp ghép những cổ phiếu tốt nhất lại với nhau không tự động tạo ra một danh mục an toàn. Chúng ta vẫn cần các nhà hoạch định chiến lược Đầu tư (Investment Strategists) và các Giám đốc Danh mục (Portfolio Managers) để phân bổ tỷ trọng, đa dạng hóa và kiểm soát rủi ro dài hạn.
        * **Đa dạng hóa Lớp tài sản:** sản phẩm này chỉ tập trung vào thị trường Cổ phiếu. Một kế hoạch quản lý và tăng trưởng tài chính vững chắc đòi hỏi sự luân chuyển linh hoạt qua các lớp tài sản khác như Trái phiếu, Bất động sản hay vàng,....

        **Tóm lại:** Cho dù máy móc có tự động hoá đến đâu, nó cũng không bao giờ thay thế được con người. Công nghệ chỉ lấy đi những phần việc tay chân lặp đi lặp lại. Nó trả lại cho các chuyên gia phân tích 90% thời gian và năng lượng để họ tập trung vào thứ mà tự động không làm được: **Suy luận, Lên kế hoạch và Xây dựng chiến lược đầu tư thực chiến.**
        """)


def render_main_content(df: pl.DataFrame, selected_q: str, updated_time: str):
    st.header(f"{selected_q}", divider="gray")
    st.caption(f"⏱️ Cập nhật lần cuối: {updated_time}")

    display_order = [
        "qmj_rank",
        "ticker",
        "company_name",
        "exchange",
        "industry_group",
        "sector_detail",
        "qmj_score",
        "qmj_profitability",
        "qmj_growth",
        "qmj_safety",
        "current_market_cap",
        "quarter_market_cap",
        "value_recent_score",
        "momentum_recent_score",
    ]

    df_display = df.select(display_order)

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
    }

    st.dataframe(
        df_display, column_config=view_config, use_container_width=True, hide_index=True
    )


# ==========================================
# 3. ORCHESTRATOR
# ==========================================
def main():
    st.set_page_config(page_title="Quantamental Screener", layout="wide")

    st.title("Hệ thống Sàng lọc Cổ phiếu Định lượng Cơ bản (Quantamental Screener)")

    st.info(
        "💡 **Lưu ý dữ liệu:** Một số doanh nghiệp chưa công bố Báo cáo tài chính Quý 1/2026. Để xem danh sách cổ phiếu được chấm điểm đầy đủ nhất, vui lòng chọn kỳ báo cáo **Quý 4/2025**."
    )
    st.header("Bảng Xếp hạng Cổ phiếu (QMJ - Value - Momentum)", divider="gray")

    df_raw = transform_data()

    if df_raw is not None:
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

        render_main_content(df_filtered, selected_q, updated_time)
    else:
        st.error("⚠️ Không tìm thấy file dữ liệu (data_qmj.csv)!")
    _introduction()
    _faq()
    _technical_adjustments()
    _limitations()
    st.markdown("---")
    st.caption("""
    **⚠️ TUYÊN BỐ MIỄN TRỪ TRÁCH NHIỆM (DISCLAIMER):** Đây là sản phẩm thực tế thuộc dự án cá nhân nhằm khẳng định năng lực chuyên môn trong lĩnh vực Data Engineering. Mọi dữ liệu, tín hiệu và thông tin trên hệ thống chỉ phục vụ mục đích tham khảo và CHẮN CHẮN có khả năng sai sót dữ liệu. Tác giả không cung cấp dịch vụ tư vấn tài chính và không chịu trách nhiệm cho bất kỳ quyết định mua bán hay tổn thất tài chính nào.
    """)


if __name__ == "__main__":
    main()
