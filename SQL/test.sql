SELECT
    "Ticker",
    year,
    quarter,
    current_assets,
    LAG(current_assets) OVER (
        PARTITION BY
            "Ticker"
        ORDER BY year ASC, quarter ASC
    ) as prev_assets,
    CASE
        WHEN current_assets = LAG(current_assets) OVER (
            PARTITION BY
                "Ticker"
            ORDER BY year ASC, quarter ASC
        ) THEN 'sai'
        ELSE 'sạch nhé'
    END as test_nhẹ
FROM raw.balance_sheet

delete from raw.balance_sheet select "Ticker", count(*) as so_luong

Select "Ticker", COUNT(*) as so_luong
From raw.balance_sheet_year
Group by
    "Ticker"
Having
    COUNT(*) < 7
order by so_luong DESC

select * from raw.balance_sheet_quarter Where "Ticker" = 'MBG'