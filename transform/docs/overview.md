# Lakehouse Data Platform Architecture

This documentation outlines the data pipeline architecture, which is organized into four distinct layers: Staging, Silver, Intermediate, and Gold. The architecture is designed to enforce data quality, traceability, and performance.

---

## 1. Staging Layer (Raw & Standardized)
The Staging layer is strictly for technical standardization. It structures raw data formats without applying business logic or dropping any records. The granularity matches the source data.

**Key Operations:**
1. **Column Standardization:** Rename columns to `snake_case`, removing whitespaces and special characters.
2. **Type Casting:** Convert raw string values into their correct analytical data types (e.g., integers, timestamps).
3. **JSON Extraction:** Parse nested properties from raw JSON strings into dedicated physical columns.
4. **Audit Columns:** Inject dbt metadata columns for basic lineage tracking.

*Note: Bad or incomplete data is not filtered out in this layer. It is passed downstream to be handled by explicit rules.*

---

## 2. Silver Layer (Cleansed & Conformed)
The Silver layer cleanses data, enforces business rules, and ensures structural integrity. It reshapes data into a reliable format for downstream modeling.

**Key Operations:**
1. **Deduplication:** Remove duplicate records or isolate the most recent record based on primary keys and update timestamps.
2. **Data Quality Validation:** Apply rigorous data quality checks. Records violating business rules are explicitly flagged (e.g., `status = 'unqualified'`).
3. **Data Reshaping:** Perform structural operations such as pivoting, unpivoting, or standardizing gaps in time-series data.
4. **Audit Columns:** Inject comprehensive metadata (e.g., `silver_updated_at`, `invocation_id`).

*Note: The Silver layer actively flags bad data. Only high-quality, validated data is permitted to advance to the calculation and presentation layers.*

---

## 3. Intermediate Layer (Business Logic & Pre-calculation)
The Intermediate layer serves as the calculation engine. It performs complex transformations and pre-calculates metrics to prevent redundant logic in the final reporting tables.

**Key Operations:**
1. **Metric Calculation:** Compute foundational business and financial factors (e.g., rolling averages, historical lags, raw Momentum and Value scores).
2. **Entity Joins:** Merge multiple cleansed Silver tables to construct wide, denormalized intermediate datasets.
3. **Logic Modularization:** Break down complex algorithms into step-by-step CTEs or separate models to maintain readable, debuggable code.

*Note: Models in this layer are for internal pipeline processing only and are not exposed directly to BI tools or end-users.*

---

## 4. Gold Layer (Business & Reporting)
The Gold layer is the presentation zone. Data here is highly refined, aggregated, and formatted specifically for business intelligence, ad-hoc analytics, and machine learning models.

**Key Operations:**
1. **Final Aggregation & Scoring:** Calculate final standardized metrics, cross-sectional rankings, and Z-scores (e.g., final QMJ scores).
2. **One Big Table (OBT):** Assemble flattened, highly optimized tables intended for direct querying by dashboards (e.g., Streamlit).
3. **Historical Snapshots (SCD Type 2):** Track historical changes over time (e.g., corporate structural changes) using validated data from this layer.

*Note: Tables in the Gold layer contain strictly qualified data and are optimized for read performance and direct consumption.*