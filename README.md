# Sales Analytics Data Pipeline
### End-to-End Data Architecture

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=flat&logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

---

## Overview

A data pipeline built for a retail sales dataset covering transactions across 2025. The project demonstrates the full analytics engineering lifecycle — from raw file ingestion through dimensional modelling to an interactive business intelligence dashboard — using the **medallion architecture** (Bronze → Silver) on Databricks with Delta Lake.

The pipeline answers seven concrete business questions about store revenue, product discounting patterns, brand performance, and regional sales rankings, with full data quality documentation at every layer.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE                                       │
│          sales_data_sampling.tsv  (2,000 rows · 20 cols)            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                                    │
│  university.bronze_sales_raw  (Delta table)                         │
│  · All 20 columns as StringType — no transformations                │
│  · Column names standardised to snake_case                          │
│  · Metadata columns added: _source_file, _load_ts, _row_num         │
│  · Every raw row preserved — nulls, bad dates, duplicates included  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                                    │
│  Star schema — 5 Delta tables + quarantine                          │
│                                                                     │
│  silver_dim_date        · 339 distinct dates · 7 columns            │
│  silver_dim_product     · SCD Type 2 · MSRP history · 10 columns    │
│  silver_dim_store       · 12 stores · surrogate key · 4 columns     │
│  silver_dim_customer    · 1,100 customers · surrogate key           │
│  silver_fact_sales      · ~1,900 clean transactions · 10 columns    │
│  silver_quarantine      · 47 rejected rows with reason codes        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BI LAYER                                        │
│  Tableau Desktop · Databricks connector · Extract mode              │
│  5 worksheets · 1 interactive dashboard                             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data platform | Databricks (Unity Catalog · database: `university`) |
| Storage format | Delta Lake (ACID · time travel · schema enforcement) |
| Processing | Apache Spark · PySpark |
| BI tool | Tableau Desktop (Extract mode) |
| Data model | Navicat — star schema reference |
| Language | Python 3 |

---

## Repository Structure

```
├── notebooks/
│   ├── 01_bronze_ingest.ipynb          # Raw ingestion — all StringType, metadata cols, 5 verification checks
│   └── 02_silver_star_schema.ipynb     # Cleanse, cast, DQ quarantine, star schema, 5 verification checks
│   └── sales_data_sampling.tsv
├── model/
│   └── Navcate.nmodel   # Navicat star schema data model
├── dashboard/
│   └── Tableau Dashboard.twbx     # Tableau workbook with embedded extract
├── docs/
│   └── Documentation.pdf     # Full technical documentation
└── README.md
```

---

## Business Questions Answered

| # | Question | Worksheet |
|---|---|---|
| BQ1 | Total revenue and tax per Store and Region on daily / weekly / monthly basis | Revenue & Tax — Monthly Trend |
| BQ2 | Which product category and sub-category combinations are most frequently discounted? | Discount Frequency |
| BQ3 | Which brand has the highest total sales volume in a specific store? | Brand Sales by Store |
| BQ4 | Validate: `Total = (Unit_Price × Qty) − Discount + Tax` | Verified in Silver notebook |
| BQ5 | Which brand had the highest total sales in the North Region during Q3? | Q3 North — Brand Ranking |
| BQ6 | Track listing price (MSRP) changes per product over time | DIM_PRODUCT SCD Type 2 |
| BQ7 | Track transaction price (actual price paid) per sale | `UnitPricePaid` in fact table |

---

## Data Quality Findings

Discovered during profiling (Silver notebook Section 0) before any transformation:

| Issue | Rows | Decision |
|---|---|---|
| Null `Transaction_ID` | 2 | Quarantined — no PK, cannot load to fact |
| Unparseable dates (`2025-13-45`, `June 12, 2025`) | 18 | Quarantined — not repaired (see note below) |
| Duplicate Transaction IDs — different content per pair | 22 | Quarantined — source system issue |
| `Quantity_Sold = 0` (void/cancelled transactions) | 5 | Quarantined |
| Null `Customer_Name` | 9 | Imputed to `'Unknown'` — Customer_ID intact |
| SKUs with multiple MSRP values | All 71 SKUs | SCD Type 2 implemented on DIM_PRODUCT |

> **Why bad dates are not repaired:** Fixing one format (`June 12, 2025`) opens infinite variation risk. Each fix is a guess about source intent, and a wrong date that passes validation silently corrupts every time-based report downstream. Corrections belong at the source system — the pipeline surfaces and quarantines bad data with a reason code, it does not silently guess.

---

## Star Schema Design

```
                    ┌─────────────────┐
                    │ silver_dim_date │
                    │  Date_Key (PK)  │
                    └────────┬────────┘
                             │ Date_Key
                             │ 
                             │                 
┌──────────────────┐  ┌──────┴──────────┐  ┌──────────────────┐
│silver_dim_product│  │silver_fact_sales│  │ silver_dim_store │
│                  │  │                 │  │                  │
│ Product_SK (PK)  │◄─┤ Product_SK (FK) │  │ Store_SK (PK)    │
│ SCD Type 2       │  │ Store_SK (FK)   ├─►│                  │
└──────────────────┘  │ Customer_SK(FK) │  └──────────────────┘
                      │ Date_Key (FK)   │
                      │ ─────────────   │  ┌─────────────────────┐
                      │ QuantitySold    │  │ silver_dim_customer │
                      │ UnitPricePaid*  │  │                     │
                      │ DiscountAmount  │  │ Customer_SK (PK)    │
                      │ Tax_Amount      ├─►│                     │
                      │ TotalSalesAmt   │  └─────────────────────┘
                      └─────────────────┘

  * UnitPricePaid is non-additive — do not SUM across rows
```

### Column inclusion philosophy

Every column in every table is justified by a business question. Columns without a BQ justification are explicitly excluded:

| Excluded | Table | Reason |
|---|---|---|
| `Store_City` | DIM_STORE | No BQ asks for city-level analysis |
| `Customer_Name` | DIM_CUSTOMER | No BQ references customers by name |
| `PaymentMethod` | FACT | No BQ involves payment method analysis |
| Formatted date strings (`Weekly`, `Monthly` etc.) | DIM_DATE | Redundant — BI tool derives these from numeric columns |

---

## SCD Type 2 — Product Dimension

Every SKU has 30–60 distinct MSRP values across 2025, confirming that listing price changes are frequent and real. SCD Type 2 is implemented with:

- `StartDate` / `EndDate` — effective date range for each price version
- `IsCurrent` — boolean flag for quickly filtering to current price without date arithmetic
- Surrogate key `Product_SK` — new row per version, stable integer FK in fact table

**Composite SCD key:** `(Product_SKU + Product_Name + Brand + Category + SubCategory)`  
SKU alone maps to many unrelated products in this dataset — using it alone would incorrectly merge unrelated SCD histories.

---

## Dashboard

Built in Tableau Desktop using the Northeastern University brand colour system:

| Colour | Hex | Usage |
|---|---|---|
| NU Red | `#D41B2C` | Hero bars (winners), negative values, brand accents |
| NU Navy | `#0C3354` | Default bars, headers, axis labels |
| NU Orange | `#E5621C` | Annotations, reference lines, callouts |
| Light grey | `#D3D1C7` | Non-highlighted bars (BI "one hero" principle) |

**BI design principles applied:**
- One hero colour per chart — the answer stands out without reading labels
- Sequential palette on heat map (white → navy) — single colour family, intensity = value
- Grey for all non-winners in ranked charts — red draws the eye to the insight
- June spike annotated — revenue is ~10× the monthly average, needs explicit callout
- Fixed filters on WS5 (Q3 + North) are protected from global dashboard filters

---

## How to Run

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Database named `university` 
- Tableau Desktop with Databricks ODBC driver installed

### Step 1 — Upload source data
Upload `sales_data_sampling.tsv` to a Databricks Volume:
```
/Volumes/university/raw_data/your_folder/sales_data_sampling.tsv
```

### Step 2 — Run Bronze notebook
Open `01_bronze_ingest.ipynb` in Databricks. Update `SOURCE_PATH` in Cell 1 to match your volume path. Run all cells. All 5 verification checks should print `PASS`.

### Step 3 — Run Silver notebook
Open `02_silver_star_schema.ipynb`. Run all cells in order. Verify:
- Row conservation: `fact + quarantine = 1,999`
- FK integrity: all four FK null counts = 0
- Formula check: 0 failures
- SCD check: 0 violations
- Business query smoke test returns Q3 North brand rankings

### Step 4 — Open the dashboard
Open `SalesAnalysis_DAMG7370.twbx` in Tableau Desktop. The extract is embedded — no Databricks connection needed to view. To refresh against live data: Data → Extract → Refresh, using your Databricks SQL Warehouse connection details.

---

## Key Design Decisions

**Bronze is immutable.** The Bronze layer never filters, transforms, or repairs data. Bad rows — including the 18 unparseable dates and 22 duplicate Transaction IDs — land in Bronze exactly as received. The pipeline surfaces problems, it does not hide them.

**Silver quarantines, never drops.** Every rejected row is written to `silver_quarantine` with a `dq_reason` code and a `_quarantine_ts` timestamp. The quarantine table is a structured feedback mechanism back to the data producer, not a trash bin.

**Schema decisions are driven by business questions.** The star schema contains no speculative columns. Every field has a documented BQ justification in the Silver notebook and in the documentation. This is the single most important discipline in dimensional modelling.

**SCD Type 2 is empirically justified.** It was not added because it is best practice — it was added because profiling confirmed that every SKU has dozens of distinct MSRP values. The code follows the evidence.

---

## License

This project is shared for educational and portfolio purposes. The source dataset is provided as part of the course assignment.
