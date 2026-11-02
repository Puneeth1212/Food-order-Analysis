# Food Orders — Batch-Based ELT Pipeline

A production-grade, batch-based ELT (Extract → Load → Transform) pipeline built with **Python**, **Pandas** and **MySQL**.  
The pipeline reads a raw CSV, preserves the original data in a raw MySQL database, cleans and enriches it using Pandas, validates the results, and loads the processed data into a separate analytics database.

---

## Project Structure

```
food_pipeline/
│
├── config.py               # All settings: file paths, batch size, DB credentials
├── extract.py              # Step 1 — Read CSV in batches of 100 rows (raw, no changes)
├── load_raw.py             # Step 2 — Load raw batches into food_orders_raw_db
├── retrieve_raw.py         # Step 3 — Retrieve raw records from MySQL in batches
├── transform.py            # Step 4 — Clean, enrich & deduplicate (cross-batch aware)
├── validate.py             # Step 5 — Validate transformed data; produce report
├── load_transformed.py     # Step 6 — Load clean records into food_orders_analytics_db
├── elt_batch_pipeline.py   # Main orchestrator — runs Steps 1-7 in order
│
├── business_analysis.sql   # SQL queries answering 6 business questions
├── requirements.txt        # Python dependencies
├── reset_mysql_password.ps1# (Optional) PowerShell helper to reset MySQL root password
│
└── reports/
    ├── validation_report.txt   # Generated after every run
    └── rejected_records.csv    # All rejected rows with their rejection reason
```

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python      | 3.10+   |
| MySQL Server| 8.0+    |
| pip packages| see `requirements.txt` |

---

## Database Setup

No manual database creation is needed.  
The pipeline **automatically creates** the following databases and tables on first run:

| Database                   | Table                |
|----------------------------|----------------------|
| `food_orders_raw_db`       | `raw_food_orders`    |
| `food_orders_analytics_db` | `cleaned_food_orders`|

---

## Configuration

Open `config.py` and update the database credentials:

```python
RAW_DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",          # ← your MySQL username
    "password": "Puneeth@1",     # ← your MySQL password
    "database": "food_orders_raw_db",
}

ANALYTICS_DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",          # ← your MySQL username
    "password": "Puneeth@1",     # ← your MySQL password
    "database": "food_orders_analytics_db",
}
```

Also verify the CSV path:

```python
CSV_FILE_PATH = r"C:\Users\punee\OneDrive\Desktop\Food\food_orders.csv"
```

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
# Windows PowerShell
$env:PYTHONUTF8="1"
python elt_batch_pipeline.py
```

> The `PYTHONUTF8=1` flag ensures UTF-8 output on Windows terminals.

---

## Pipeline Steps

| Step | Module              | Description |
|------|---------------------|-------------|
| 1    | `extract.py`        | Reads `food_orders.csv` in 100-row chunks; no transformation |
| 2    | `load_raw.py`       | Creates `food_orders_raw_db.raw_food_orders`; inserts each batch in a transaction; preserves NULLs & duplicates |
| 3    | `retrieve_raw.py`   | Fetches rows from the raw table in 100-row LIMIT/OFFSET pages |
| 4    | `transform.py`      | Deduplicates (intra + cross-batch), imputes missing values, converts types, computes derived columns |
| 5    | `validate.py`       | Checks for nulls, duplicates, dtypes, delivery time ≥ 0, revenue formula; writes report & rejected CSV |
| 6    | `load_transformed.py` | Inserts valid records into `food_orders_analytics_db.cleaned_food_orders` using `INSERT IGNORE` |
| 7    | `elt_batch_pipeline.py` | Orchestrates all steps; prints final execution report |

---

## Transformation Decisions

| Column | Strategy |
|--------|----------|
| `Order Value` | Impute with **global median** (1041.0) |
| `Delivery Fee` | Impute with **0.0** (free delivery assumed) |
| `Commission Fee` | Impute with **global median** (128.0) |
| `Payment Processing Fee` | Impute with **global median** (30.0) |
| `Refunds/Chargebacks` | Impute with **0.0** (no refund assumed) |
| `Payment Method` | Fill `NaN` → `"Unknown"` |
| `Discounts and Offers` | Fill `NaN` → `"None"` |
| Timestamps | `pd.to_datetime(..., errors='coerce')` → unparseable → **rejected** |
| `Delivery_Time_Minutes` | `(Delivery DT − Order DT).total_seconds() / 60` |
| `Platform_Net_Revenue` | `Commission Fee + Delivery Fee − Payment Processing Fee − Refunds/Chargebacks` |
| Duplicates | Key = `Order ID`; cross-batch duplicates tracked in a module-level set → **rejected** |

---

## Rejection Reasons

Records are rejected (saved to `reports/rejected_records.csv`) for:

- **Cross-batch duplicate Order ID** — same Order ID appeared in a previous batch
- **Invalid or missing timestamp** — `NaT` after `pd.to_datetime()` coercion
- **Negative delivery duration** — `Delivery_Time_Minutes < 0`
- **Remaining NaN in financial columns after imputation**

---

## Sample Execution Results

```
╔══════════════════════════════════════════════════════════╗
║   FOOD ORDERS — ELT BATCH PIPELINE                      ║
║   Batch size:  100 rows                                  ║
╚══════════════════════════════════════════════════════════╝

  Global medians computed: {Order Value: 1041.0, Commission Fee: 128.0, Payment Processing Fee: 30.0}

STEP 2 — Loading raw data into MySQL (food_orders_raw_db)
  Raw DB ready: food_orders_raw_db.raw_food_orders
  Batch   1 |  100 rows inserted into raw DB ✓
  ...
  Batch  11 |   10 rows inserted into raw DB ✓
  Total rows loaded : 1010 | DB count verified : 1010

STEP 4+5+6 — Transform, Validate & Load (streaming)
  Batch  11: rejected 10 cross-batch duplicate(s)

╔══════════════════════════════════════════════════════════╗
║   FINAL EXECUTION REPORT                                ║
╠══════════════════════════════════════════════════════════╣
║  Total records in CSV                        : 1010      ║
║  Total batches extracted                     : 11        ║
║  Total raw records loaded into MySQL         : 1010      ║
║  Total raw records retrieved from MySQL      : 1010      ║
║  Total valid records after transform         : 1000      ║
║  Total rejected records                      : 10        ║
║  Total records loaded into analytics DB      : 1000      ║
║  Analytics DB verified count                 : 1000      ║
║  Successful load batches                     : 11        ║
║  Failed load batches                         : 0         ║
╠══════════════════════════════════════════════════════════╣
║  Validation status : PASSED WITH WARNINGS ⚠              ║
║  Pipeline duration : 1.6s                                ║
╚══════════════════════════════════════════════════════════╝
```

---

## Business Analysis SQL Results

Run `business_analysis.sql` against `food_orders_analytics_db`:

```bash
mysql -u root -p food_orders_analytics_db < business_analysis.sql
```

| Question | Result |
|----------|--------|
| Total Order Value | **₹1,055,055.00** |
| Average Order Value | **₹1,055.06** |
| Top Payment Method | **Cash on Delivery** (336 orders) |
| Top Order Channel | **Mobile App** (686 orders) |
| Average Delivery Time | **73.58 minutes** |
| Total Platform Net Revenue | **₹97,789.00** |

---

## Validation Report

After every pipeline run, two files are generated in `reports/`:

- **`validation_report.txt`** — full summary with batch-level issues, rejection reasons and final status
- **`rejected_records.csv`** — all rejected rows with a `Rejection_Reason` column

---

## Submission Contents

| File | Description |
|------|-------------|
| `config.py` | Configuration |
| `extract.py` | CSV extraction |
| `load_raw.py` | Raw DB loader |
| `retrieve_raw.py` | Raw DB retriever |
| `transform.py` | Transformation logic |
| `validate.py` | Validation & reporting |
| `load_transformed.py` | Analytics DB loader |
| `elt_batch_pipeline.py` | Main orchestrator |
| `business_analysis.sql` | 6 SQL queries |
| `requirements.txt` | Dependencies |
| `reports/validation_report.txt` | Generated report |
| `reports/rejected_records.csv` | Rejected records |
| `README.md` | This file |
