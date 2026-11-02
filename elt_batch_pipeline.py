# -*- coding: utf-8 -*-
# =============================================================================
# elt_batch_pipeline.py — Orchestrate the complete ELT pipeline
# =============================================================================
# Execution order:
#   1. Extract CSV → batches
#   2. Load raw batches → food_orders_raw_db
#   3. Retrieve raw batches ← food_orders_raw_db
#   4. Transform each batch (Pandas)
#   5. Validate each transformed batch
#   6. Load valid records → food_orders_analytics_db
#   7. Print final execution report
# =============================================================================

import sys
import io
import time

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
import pandas as pd

from config import CSV_FILE_PATH, BATCH_SIZE
from extract import extract_batches, count_total_records
from load_raw import load_all_raw_batches
from retrieve_raw import retrieve_raw_batches
from transform import transform_batch, reset_seen_order_ids, set_global_medians
from validate import validate_batch, ValidationReporter
from load_transformed import setup_analytics_database, load_transformed_batch, verify_analytics_count


def run_pipeline():
    pipeline_start = time.time()
    print()
    print("╔" + "═" * 58 + "╗")
    print("║   FOOD ORDERS — ELT BATCH PIPELINE                      ║")
    print("║   Batch size: {:>4} rows                                  ║".format(BATCH_SIZE))
    print("╚" + "═" * 58 + "╝")
    print()

    # -----------------------------------------------------------------------
    # STEP 1 — Extract
    # -----------------------------------------------------------------------
    # Pre-compute global medians from the full CSV for consistent imputation
    full_df = pd.read_csv(CSV_FILE_PATH, dtype=str)
    set_global_medians(full_df)
    total_csv_records = len(full_df)
    del full_df  # release memory

    extract_gen = extract_batches()   # generator (lazy)

    # -----------------------------------------------------------------------
    # STEP 2 — Load raw (consumes the extract generator)
    # -----------------------------------------------------------------------
    raw_stats = load_all_raw_batches(extract_gen)

    # -----------------------------------------------------------------------
    # STEP 3-6 — Retrieve → Transform → Validate → Load (streaming pipeline)
    # -----------------------------------------------------------------------
    print("=" * 60)
    print("STEP 4+5+6 — Transform, Validate & Load (streaming)")
    print("=" * 60)

    reset_seen_order_ids()        # fresh run → clear cross-batch memory
    setup_analytics_database()   # ensure analytics table exists
    print("-" * 60)

    reporter = ValidationReporter()

    total_valid_records    = 0
    total_rejected_records = 0
    total_loaded_analytics = 0
    successful_load_batches = 0
    failed_load_batches     = 0
    total_retrieve_batches  = 0
    total_retrieved_records = 0

    for batch_number, raw_batch_df in retrieve_raw_batches():
        total_retrieve_batches  += 1
        total_retrieved_records += len(raw_batch_df)

        # -- Transform --
        valid_df, rejected_df = transform_batch(batch_number, raw_batch_df)

        # -- Validate --
        val_result = validate_batch(batch_number, valid_df, rejected_df)
        reporter.add_batch(val_result, rejected_df)

        total_valid_records    += len(valid_df)
        total_rejected_records += len(rejected_df)

        # -- Load --
        load_result = load_transformed_batch(batch_number, valid_df)
        if load_result["success"]:
            total_loaded_analytics += load_result["inserted"]
            successful_load_batches += 1
        else:
            failed_load_batches += 1

    db_analytics_count = verify_analytics_count()

    # -----------------------------------------------------------------------
    # STEP 7 — Validation report
    # -----------------------------------------------------------------------
    pipeline_stats = {
        "Total records in CSV":                         total_csv_records,
        "Total batches extracted":                      raw_stats["successful_batches"] + raw_stats["failed_batches"],
        "Total raw records loaded into MySQL":          raw_stats["total_loaded"],
        "Total raw records retrieved from MySQL":       total_retrieved_records,
        "Total valid records after transform":          total_valid_records,
        "Total rejected records":                       total_rejected_records,
        "Total records loaded into analytics DB":       total_loaded_analytics,
        "Analytics DB verified count":                  db_analytics_count,
        "Successful load batches":                      successful_load_batches,
        "Failed load batches":                          failed_load_batches,
        "Raw DB — successful batches":                  raw_stats["successful_batches"],
        "Raw DB — failed batches":                      raw_stats["failed_batches"],
    }

    overall_ok = reporter.write_report(pipeline_stats)

    # -----------------------------------------------------------------------
    # STEP 8 — Final execution report (console)
    # -----------------------------------------------------------------------
    elapsed = time.time() - pipeline_start
    print()
    print("╔" + "═" * 58 + "╗")
    print("║   FINAL EXECUTION REPORT                                ║")
    print("╠" + "═" * 58 + "╣")
    for key, val in pipeline_stats.items():
        label = f"  {key}"
        line  = f"║{label:<45} : {str(val):<10}║"
        print(line)
    print("╠" + "═" * 58 + "╣")
    status_str = "PASSED ✓" if overall_ok else "PASSED WITH WARNINGS ⚠"
    print(f"║  Validation status : {status_str:<36}║")
    print(f"║  Pipeline duration : {elapsed:.1f}s{' ' * 35}║")
    print("╚" + "═" * 58 + "╝")
    print()


if __name__ == "__main__":
    run_pipeline()
