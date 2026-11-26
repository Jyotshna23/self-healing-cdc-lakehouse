# 🛡️ Zero-Infrastructure Self-Healing CDC Lakehouse

[![Self-Healing Lakehouse CI](https://github.com)](https://github.com)
![Python](https://shields.io)
![Polars](https://shields.io)
![DuckDB](https://shields.io)
![Pydantic](https://shields.io)
![License](https://shields.io)

> **Engineered for Resource Efficiency & Production Reliability:** An end-to-end incremental **Change Data Capture (CDC) & Lakehouse Data Engineering pipeline** built with embedded, single-node tools (**$0 Cloud Spend**) featuring data contracts, automated quarantine gates, Slowly Changing Dimensions (**SCD Type 2**), and CI/CD quality verification.

---

## 📋 Table of Contents
1. [Problem Statement & Engineering Motivation](#-problem-statement--engineering-motivation)
2. [System Architecture & Data Lineage](#-system-architecture--data-lineage)
3. [Core Engineering Pillars](#-core-engineering-pillars)
4. [Technology Stack & Architectural Trade-Offs](#-technology-stack--architectural-trade-offs)
5. [Project Directory Structure](#-project-directory-structure)
6. [Data Schema & Quarantine Contract](#-data-schema--quarantine-contract)
7. [Quickstart & Local Execution](#-quickstart--local-execution)
8. [Sample Pipeline Execution Output](#-sample-pipeline-execution-output)
9. [Automated CI/CD & Data Quality Gates](#-automated-cicd--data-quality-gates)
10. [Future Roadmap](#-future-roadmap)

---

## 🎯 Problem Statement & Engineering Motivation

In modern enterprise data platforms, pipelines frequently fail or silently corrupt downstream analytics due to three recurring issues:
1. **Unannounced Upstream Schema/Value Drift:** Transactional databases often emit negative prices, malformed emails, or invalid status strings that crash batch jobs at 2:00 AM.
2. **Expensive Full-Table Reloads:** Re-ingesting entire source tables wastes compute, I/O, and memory.
3. **Heavy Infrastructure Dependencies:** Many reference architectures rely on JVM clusters (Spark/Kafka) that require complex setup and high overhead for local testing on standard developer laptops.

**The Solution:** This project implements a **zero-infrastructure, self-healing modern data stack** that processes incremental delta batches, isolates bad records without halting execution, tracks full historical state changes, and validates every commit automatically via GitHub Actions.

---

## 🏗️ System Architecture & Data Lineage

~~~mermaid
flowchart LR
    subgraph Source ["1. OLTP Transactional Layer"]
        A[(SQLite OLTP DB\necommerce.db)]
    end

    subgraph Ingestion ["2. Incremental CDC Extraction"]
        W[(watermark.json\nStateful Cursor)] -.->|last_updated_at| B[Polars Delta Extractor]
        A -->|WHERE updated_at > cursor| B
    end

    subgraph Quality ["3. Self-Healing Data Contract"]
        B --> C{Pydantic V2\nValidation Gate}
        C -->|Valid Schema & Rules| D[Compressed Parquet\nBronze/Silver Lakehouse]
        C -->|Corrupt / Anomaly Rows| E[Quarantine Parquet\n+ Error Audit Metadata]
    end

    subgraph Warehouse ["4. Analytical Modeling"]
        D -->|SCD Type 2 Merge| F[(DuckDB Warehouse\ndim_orders_scd2)]
    end

    subgraph Testing ["5. Automated Governance"]
        F --> G[PyTest SQL Quality Gates]
        E --> G
        G --> H[GitHub Actions CI/CD]
    end
~~~

---

## 🚀 Core Engineering Pillars

### 1. Stateful Incremental CDC (Change Data Capture)
* Instead of performing full-table scans on every run, the pipeline maintains a persistent state cursor in `data/lakehouse/watermark.json`.
* On each run, **Polars** queries only records where `updated_at > last_updated_at`, avoiding redundant reprocessing of historical rows.
* Upon successful warehouse ingestion, the watermark advances to the batch's maximum `updated_at` timestamp.

### 2. Self-Healing Quarantine Gate (Data Contracts)
* Every incoming record is validated against a strict **Pydantic** data contract (`OrderContract`) before entering the Lakehouse.
* **Enforced Rules:**
  * `order_id`: Must be a valid integer primary key.
  * `customer_email`: Must conform to RFC-compliant email formatting (`EmailStr`).
  * `amount`: Must be strictly greater than zero (`amount > 0`).
  * `status`: Must match allowed domain states (`PLACED`, `SHIPPED`, `DELIVERED`).
* **Zero-Downtime Isolation:** When corrupted rows are detected, the pipeline **does not crash**. Instead, it enriches invalid records with diagnostic metadata (`error_field`, `error_reason`, `quarantined_at`) and routes them to `data/quarantine/bad_<batch_id>.parquet` for engineering triage.

### 3. Slowly Changing Dimensions (SCD Type 2)
* Analytical business intelligence requires knowing not just the *current* state of an order, but its *complete lifecycle history* (e.g., when an order transitioned from `PLACED` to `DELIVERED` or changed price).
* Inside **DuckDB**, incoming delta updates automatically expire prior active records (`is_current = FALSE`, populating `valid_to`) and insert the new active version (`is_current = TRUE`, `valid_to = NULL`).

### 4. Lightweight Local-First Execution
* Built entirely on embedded engines (SQLite, Polars, DuckDB) with zero external server or cluster requirements, allowing the full end-to-end pipeline to run cleanly on standard developer hardware.

---

## 🛠️ Technology Stack & Architectural Trade-Offs

| Layer | Technology | Why It Was Chosen Over Alternatives |
| :--- | :--- | :--- |
| **Source OLTP** | **SQLite** | Zero-configuration transactional database ideal for simulating real-world e-commerce inserts and updates locally. |
| **Extraction & ETL** | **Polars** | Rust-backed, multi-threaded DataFrame engine with an Apache Arrow memory layout for fast, lightweight local transformations. |
| **Data Contracts** | **Pydantic** | Industry-standard type and rule enforcement with granular field-level error reporting for quarantine diagnostics. |
| **Storage Format** | **Apache Parquet** | Columnar, compressed storage providing immutable batch snapshots and efficient analytical scans. |
| **OLAP Warehouse** | **DuckDB** | Embedded in-process analytical SQL engine capable of vectorized SCD Type 2 merges without external server overhead. |
| **Testing & CI/CD** | **PyTest + GitHub Actions** | Automates end-to-end pipeline verification on every `git push` in an isolated Ubuntu runner. |

---

## 📂 Project Directory Structure

~~~text
self-healing-cdc-lakehouse/
├── .github/
│   └── workflows/
│       └── ci.yml                 # Automated CI/CD pipeline executing full & delta runs + pytest
├── tests/
│   └── test_quality.py            # Automated SQL & file-level data quality assertions
├── data/                          # Git-ignored runtime storage layer
│   ├── raw/
│   │   └── ecommerce.db           # Simulated SQLite OLTP transactional source
│   ├── lakehouse/
│   │   ├── clean_*.parquet        # Validated, immutable Parquet lakehouse partitions
│   │   ├── warehouse.duckdb       # Analytical DuckDB database housing SCD2 tables
│   │   └── watermark.json         # Persistent CDC state cursor
│   └── quarantine/
│       └── bad_*.parquet          # Isolated anomaly records with audit trail metadata
├── generate_source.py             # Synthetic OLTP workload & anomaly chaos generator
├── pipeline.py                    # Core incremental CDC, Pydantic gate, & DuckDB SCD2 engine
├── requirements.txt               # Lightweight Python dependencies
└── README.md                      # Architecture & engineering documentation
~~~

---

## 📐 Data Schema & Quarantine Contract

### Analytical Warehouse Table (`dim_orders_scd2`)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `order_id` | `INTEGER` | Natural business key of the e-commerce order |
| `customer_email` | `VARCHAR` | Validated customer contact email |
| `product_sku` | `VARCHAR` | Product stock-keeping unit identifier |
| `amount` | `DOUBLE` | Validated transaction amount (`> 0.0`) |
| `status` | `VARCHAR` | Order lifecycle state (`PLACED`, `SHIPPED`, `DELIVERED`) |
| `valid_from` | `VARCHAR` | ISO-8601 timestamp when this record version became active |
| `valid_to` | `VARCHAR` | ISO-8601 timestamp when superseded (`NULL` if active) |
| `is_current` | `BOOLEAN` | Flag indicating the latest active record (`TRUE` / `FALSE`) |

### Quarantine Audit Columns (`bad_*.parquet`)
In addition to all original source columns, quarantined records automatically receive:
* `error_field`: Exact column that failed contract validation (e.g., `amount`, `customer_email`, `status`).
* `error_reason`: Human-readable validation failure description (e.g., `Input should be greater than 0`).
* `quarantined_at`: UTC execution timestamp when the anomaly was intercepted.

---

## ⚡ Quickstart & Local Execution

### 1. Clone the Repository
~~~bash
git clone https://github.com
cd self-healing-cdc-lakehouse
~~~

### 2. Create Virtual Environment & Install Dependencies
~~~bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
mkdir -p data/raw data/lakehouse data/quarantine
~~~

### 3. Run Batch 1: Initial Historical Load (with Injected Anomalies)
~~~bash
python generate_source.py initial
python pipeline.py
~~~

### 4. Run Batch 2: Incremental CDC Delta Load (Updates + New Inserts)
~~~bash
python generate_source.py delta
python pipeline.py
~~~

### 5. Execute Automated Data Quality Tests
~~~bash
pytest -v
~~~

---

## 📊 Sample Pipeline Execution Output

When running the initial batch followed by an incremental CDC delta batch, the pipeline produces clear operational telemetry *(exact counts and revenue totals vary slightly per run due to randomized status and amount generation in `generate_source.py`)*:

~~~text
$ python generate_source.py initial
Batch 1: Generated 5,000 initial orders with injected schema/data anomalies.

$ python pipeline.py
CDC Processed: 5000 | Clean: 4700 | Quarantined: 300
SCD2 Historical Versions Archived: 0
Active Warehouse Metrics: [('DELIVERED', 1568, 403112.40), ('PLACED', 1561, 401890.15), ('SHIPPED', 1571, 405234.80)]

$ python generate_source.py delta
Batch 2 (CDC Delta): Updated 50 existing orders + inserted 100 new orders.

$ python pipeline.py
CDC Processed: 150 | Clean: 150 | Quarantined: 0
SCD2 Historical Versions Archived: 47
Active Warehouse Metrics: [('DELIVERED', 1601, 412450.22), ('PLACED', 1661, 426110.90), ('SHIPPED', 1538, 396780.45)]

$ pytest -v
tests/test_quality.py::test_lakehouse_files_created PASSED
tests/test_quality.py::test_no_corrupt_data_in_warehouse PASSED
tests/test_quality.py::test_scd2_uniqueness_and_history PASSED
============================== 3 passed in 0.42s ==============================
~~~

---

## ✅ Automated CI/CD & Data Quality Gates

Every push to `main` triggers `.github/workflows/ci.yml`, which spins up an Ubuntu environment and validates:
1. **Lakehouse & Quarantine Partition Generation:** Confirms both valid (`clean_*.parquet`) and rejected (`bad_*.parquet`) files are written properly.
2. **Zero-Corruption Warehouse Guarantee:** Runs SQL assertions against `dim_orders_scd2` verifying `0` records exist with `amount <= 0` or `customer_email IS NULL`.
3. **SCD Type 2 Referential Integrity:** Confirms every `order_id` has **at most one** active row (`is_current = TRUE`) and that historical updates are properly archived (`is_current = FALSE`).

---

## 🔮 Future Roadmap
- [ ] **Schema Evolution Support:** Automatically detect newly added upstream columns and evolve Parquet/DuckDB schemas dynamically.
- [ ] **Dead-Letter Replay CLI:** Build a recovery script to re-ingest quarantined records once upstream data bugs are patched.
- [ ] **Interactive Streamlit Observability Dashboard:** Add a lightweight local UI to visualize quarantine error distributions and revenue trends.

---

## 👩‍💻 Author

**Jyotshna Pogiri**  
*Data Engineering & Python Developer*  
- **GitHub:** [@Jyotshna23](https://github.com)  
- **LinkedIn:** [Jyotshna Pogiri](https://linkedin.com)  
- **Email:** jahnavipogiri3@gmail.com

