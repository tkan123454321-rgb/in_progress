# Quantamental Factor Screener: An Automated Financial Data Platform
## The Real Problem: It's About time, not data
The biggest pain in long-term investing is **NOT** a lack of data. It is **NOT** a lack of valuation methods or formulas.

The true pain point is the **MASSIVE AMOUNT OF TIME** wasted on manual work.

Hand-picking and analyzing individual companies across a market of 1,600+ stocks takes **WEEKS, EVEN MONTHS**. You can spend days digging through financial statements and calculating ratios in Excel, only to realize the company is fundamentally weaked. All that time and effort are completely wasted.

## Solution: Automating the Heavy Lifting
This platform was built to solve that exact pain point. By automating the data engineering pipeline, it **SAVES 90% OF YOUR RESEARCH TIME**. It acts as an automated screener that instantly narrows down the entire market by answering three core questions in a sequential pipeline:

*   **1. "Is this a fundamentally strong business?" (Solved by Quality - QMJ Score):**
    *   **The Pain:** Spending days manually reading through hundreds of financial reports, struggling to find a few truly high-quality companies among thousands of options.
    *   **The Solution:** The QMJ score is your **AUTOMATED QUALITY SCREENER**. It objectively evaluates financial health and hands you a watchlist of ONLY the safest, most profitable, and consistently growing companies. This forms the foundation of your research.

*   **2. "Is the price fair?" (Solved by Value Score):**
    *   **The Pain:** Spending hours building complex valuation models, only to find out a quality stock is currently too expensive.
    *   **The Solution:** Building strictly on top of your QMJ-approved list, the Value score **DOES THE MATH FOR YOU**. It instantly scans your pre-filtered high-quality companies and highlights which ones are currently undervalued, ensuring you never overpay for a good asset.

*   **3. "Is it time to act?" (Solved by Momentum Score):**
    *   **The Pain:** Buying a great, undervalued stock, but waiting years for the price to move because the broader market is ignoring it.
    *   **The Solution:** The Momentum score **TRACKS THE TREND**. As the final overlay, it shows you exactly which of those high-quality, attractively priced stocks are attracting cash flow *right now*, helping you optimize your entry timing.

## ⚠️ Limitations: The Irreplaceable Human Element

While this platform successfully automates data collection and screening, it is a **tool to help make decisions, not a replacement for human experts**.

Here is what this platform *cannot* replace:

*   **Data Imperfections:** Automated systems rely on raw data, which can sometimes have reporting errors or missing numbers. The screened list is just step one, not the final answer.
*   **Deep Fundamental Analysis:** The system calculates past and present numbers perfectly, but it cannot predict the future. We still absolutely need **Financial Analysts** to dig deep into a company's real-world business model, competitive advantages, and true future potential.
*   **Strategic Portfolio Management:** A list of good stocks doesn't automatically create a safe portfolio. We still need **Investment Strategists** to set the big-picture goals, and **Portfolio Managers** to build a balanced portfolio, manage daily risks, and track long-term growth.
*   **Asset Diversification:** This platform focuses only on stocks. A solid investment plan needs diversification across other asset classes like bonds, real estate, or commodities.

> *No matter how smart the machine gets, it cannot replace finance professionals. Technology just takes away the manual, repetitive work. This gives human analysts more time and energy to focus on what they do best: thinking, planning, and making real strategies.*

## See It In Action (Live Demo)

All the heavy data processing from the pipeline is served directly to a live, interactive dashboard. You can jump right in to see how the system automatically ranks and filters the market.

👉 **[CLICK HERE TO EXPLORE THE LIVE QUANTAMENTAL SCREENER](https://qmj-dashboard.streamlit.app/)** 👈
> **Note on "Cold Start":** Since this is a community-hosted app, it may go into "sleep mode" if inactive. If the page doesn't load immediately, please **click the "Wake up" button and wait about 30 seconds** for the system to boot up.

*(Tip: Once inside, please select the **Q4/2025** reporting period—since many companies haven't released their Q1/2026 financial reports yet. Then, filter by "Hạng QMJ" to instantly see the top fundamentally strong companies in the market).*

---
*Alright folks! let's move to the data platform architecture to clearly see the engine under the hood*
# System Architecture
![Platform Architecture](./assets/images/architecture.png)

> *Architecture showing a end-to-end data pipeline platform: from raw financial data ingestion to the final automated screening dashboard.*

---
