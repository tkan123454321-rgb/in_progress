{% macro check_value_and_momentum_column(factor_type) %}

    {% if factor_type == 'value' %}
        NULLIF(
            CONCAT_WS(' | ',
                CASE WHEN gap_6m IS NULL OR gap_6m != 2 THEN 'Err: Missing 6-month lag for Book Equity (Gap != 2)' ELSE NULL END,
                CASE WHEN book_equity_6m_lag IS NULL THEN 'Err: Lagged Book Equity is null' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'momentum' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- Bắt buộc phải có cả Gap 1m và Gap 12m
                CASE WHEN gap_12m IS NULL OR gap_12m != 12 THEN 'Err: Missing 12-month lag for Price (Gap != 12)' ELSE NULL END,
                
                -- Đảm bảo giá không bị Null hoặc bằng 0 (để tránh lỗi chia cho 0)
                CASE WHEN price_t_1 IS NULL THEN 'Err: Lagged Price 1m is null' ELSE NULL END,
                CASE WHEN price_t_12 IS NULL OR price_t_12 = 0 THEN 'Err: Lagged Price 12m is null or zero' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'momentum_recent' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. BỘ LỌC TƯƠI SỐNG (FRESHNESS CHECK): Quá 10 ngày không có giao dịch -> Trảm
                -- (Dùng CAST(date AS DATE) để đảm bảo Trino không bị lỗi nếu cột date đang là Timestamp)
                CASE WHEN DATE_DIFF('day', CAST(date AS DATE), CURRENT_DATE) > 10 THEN 'Err: Stale Data (Suspended or no trades > 10 days)' ELSE NULL END,
                
                -- 2. BỘ LỌC 1 THÁNG (21 PHIÊN): Phải có gốc so sánh ngắn hạn
                CASE WHEN price_t_21 IS NULL THEN 'Err: Missing 1-month benchmark (price_t_21 is null or 0)' ELSE NULL END,
                
                -- 3. BỘ LỌC 1 NĂM (252 PHIÊN): Không đủ tuổi niêm yết hoặc mất thanh khoản -> Trảm
                CASE WHEN price_t_252 IS NULL THEN 'Err: Insufficient 1-year history (price_t_252 is null or 0)' ELSE NULL END,
                CASE WHEN momentum_recent IS NULL THEN 'Err: Momentum calculation failed (momentum_recent is null)' ELSE NULL END
            ), 
        '')
    {% elif factor_type == 'value_recent' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- 1. BỘ LỌC TƯƠI SỐNG CHO GIÁ (> 10 NGÀY -> TRẢM)
                CASE WHEN days_since_update > 10 THEN 'Err: Stale Data in gold_dim_company (> 10 days)' ELSE NULL END,
                
                -- 2. BỘ LỌC TƯƠI SỐNG CHO BÁO CÁO TÀI CHÍNH (> 2 QUÝ -> TRẢM)
                CASE WHEN quarters_delayed > 2 THEN 'Err: Stale Fundamental Data (Delayed > 2 Quarters)' ELSE NULL END,
                
                -- 4. KIỂM TRA TÀI SẢN (BOOK EQUITY)
                CASE WHEN value_recent_score IS NULL THEN 'Err: Invalid Value Recent Score' ELSE NULL END
            ), 
        '')
        
    {% endif %}

{% endmacro %}

{% macro check_value_and_momentum_z_score_column(factor_type) %}

    {% if factor_type == 'value_momentum_z' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- CHECK NULL CHO 2 Z-SCORE QUAN TRỌNG NHẤT
                CASE WHEN z_value IS NULL THEN 'z_value is null' ELSE NULL END,
                CASE WHEN z_momentum IS NULL THEN 'z_momentum is null' ELSE NULL END
             ), 
        '')
    
    {% elif factor_type == 'value_momentum_z_recent' %}
        NULLIF(
            CONCAT_WS(' | ',
                -- CHECK NULL CHO 2 Z-SCORE QUAN TRỌNG NHẤT
                CASE WHEN z_value_recent IS NULL THEN 'z_value_recent is null' ELSE NULL END,
                CASE WHEN z_momentum_recent IS NULL THEN 'z_momentum_recent is null' ELSE NULL END
             ), 
        '')
    {% endif %}

{% endmacro %}