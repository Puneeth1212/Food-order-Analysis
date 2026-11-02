# =============================================================================
# validate.py — Validate transformed DataFrames and produce a report
# =============================================================================
# Performs a series of checks on the cleaned data, accumulates results across
# all batches, and writes a final validation report + rejected records CSV.
# =============================================================================

import os
import pandas as pd
import numpy as np
from datetime import datetime
from config import (
    COL_ORDER_ID, COL_ORDER_DT, COL_DELIVERY_DT,
    COL_ORDER_VALUE, COL_DELIVERY_FEE, COL_COMMISSION_FEE,
    COL_PAYMENT_PROC_FEE, COL_REFUNDS,
    COL_DELIVERY_TIME_MINS, COL_PLATFORM_NET_REV,
    VALIDATION_REPORT_PATH, REJECTED_RECORDS_PATH,
)


# ---------------------------------------------------------------------------
# Per-batch validation
# ---------------------------------------------------------------------------
def validate_batch(batch_number: int, valid_df: pd.DataFrame,
                   rejected_df: pd.DataFrame) -> dict:
    """
    Run validation checks on a single transformed batch and return a summary.

    Checks performed
    ----------------
    1. Remaining missing values in valid_df
    2. Duplicate Order IDs within the batch
    3. Data types of key columns
    4. Delivery_Time_Minutes >= 0
    5. Platform_Net_Revenue matches the formula
    """
    results = {
        "batch_number":         batch_number,
        "valid_count":          len(valid_df),
        "rejected_count":       len(rejected_df),
        "issues":               [],
    }

    if valid_df.empty:
        results["issues"].append("All records rejected — empty valid DataFrame")
        return results

    # 1. Remaining NaNs
    null_counts = valid_df.isnull().sum()
    null_cols   = null_counts[null_counts > 0]
    if not null_cols.empty:
        results["issues"].append(
            f"Remaining NaNs in: {null_cols.to_dict()}"
        )

    # 2. Intra-batch duplicate Order IDs
    dup_count = valid_df.duplicated(subset=[COL_ORDER_ID]).sum()
    if dup_count > 0:
        results["issues"].append(
            f"{dup_count} duplicate Order ID(s) found in valid records"
        )

    # 3. Data type checks
    expected_dtypes = {
        COL_DELIVERY_TIME_MINS: "float64",
        COL_PLATFORM_NET_REV:   "float64",
        COL_ORDER_VALUE:        "float64",
    }
    for col, expected in expected_dtypes.items():
        if col in valid_df.columns:
            actual = str(valid_df[col].dtype)
            if not actual.startswith("float"):
                results["issues"].append(
                    f"Column '{col}': expected float, got {actual}"
                )

    # 4. Delivery time validation
    if COL_DELIVERY_TIME_MINS in valid_df.columns:
        neg_count = (valid_df[COL_DELIVERY_TIME_MINS] < 0).sum()
        if neg_count > 0:
            results["issues"].append(
                f"{neg_count} record(s) with negative Delivery_Time_Minutes"
            )

    # 5. Platform net revenue formula verification
    if all(c in valid_df.columns for c in [
        COL_COMMISSION_FEE, COL_DELIVERY_FEE, COL_PAYMENT_PROC_FEE,
        COL_REFUNDS, COL_PLATFORM_NET_REV
    ]):
        expected_rev = (
            valid_df[COL_COMMISSION_FEE]
            + valid_df[COL_DELIVERY_FEE]
            - valid_df[COL_PAYMENT_PROC_FEE]
            - valid_df[COL_REFUNDS]
        ).round(2)
        mismatch = (~np.isclose(
            valid_df[COL_PLATFORM_NET_REV].fillna(0),
            expected_rev.fillna(0),
            atol=0.01
        )).sum()
        if mismatch > 0:
            results["issues"].append(
                f"{mismatch} record(s) with Platform_Net_Revenue formula mismatch"
            )

    return results


# ---------------------------------------------------------------------------
# Aggregate report writer
# ---------------------------------------------------------------------------
class ValidationReporter:
    """Accumulates per-batch validation results and writes the final report."""

    def __init__(self):
        self.batch_results  = []
        self.all_rejected   = []

    def add_batch(self, batch_result: dict, rejected_df: pd.DataFrame):
        self.batch_results.append(batch_result)
        if not rejected_df.empty:
            self.all_rejected.append(rejected_df)

    def write_report(self, pipeline_stats: dict):
        """Write validation_report.txt and rejected_records.csv."""

        # --- Rejected records CSV ---
        if self.all_rejected:
            rejected_combined = pd.concat(self.all_rejected, ignore_index=True)
        else:
            rejected_combined = pd.DataFrame()

        if not rejected_combined.empty:
            rejected_combined.to_csv(REJECTED_RECORDS_PATH, index=False)
            print(f"  Rejected records saved → {REJECTED_RECORDS_PATH}")

        # --- Text report ---
        total_valid    = sum(r["valid_count"]    for r in self.batch_results)
        total_rejected = sum(r["rejected_count"] for r in self.batch_results)
        batches_with_issues = [r for r in self.batch_results if r["issues"]]

        lines = []
        lines.append("=" * 70)
        lines.append("FOOD ORDERS ELT PIPELINE — VALIDATION REPORT")
        lines.append(f"Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append("")
        lines.append("PIPELINE SUMMARY")
        lines.append("-" * 70)
        for key, val in pipeline_stats.items():
            lines.append(f"  {key:<40} : {val}")
        lines.append("")
        lines.append("VALIDATION SUMMARY")
        lines.append("-" * 70)
        lines.append(f"  Total batches validated          : {len(self.batch_results)}")
        lines.append(f"  Total valid records              : {total_valid}")
        lines.append(f"  Total rejected records           : {total_rejected}")
        lines.append(f"  Batches with issues              : {len(batches_with_issues)}")
        lines.append(f"  Rejected records CSV             : {REJECTED_RECORDS_PATH}")
        lines.append("")

        if batches_with_issues:
            lines.append("BATCH-LEVEL ISSUES")
            lines.append("-" * 70)
            for r in batches_with_issues:
                lines.append(f"  Batch {r['batch_number']:>3}:")
                for issue in r["issues"]:
                    lines.append(f"    • {issue}")
            lines.append("")

        # Rejection reason breakdown
        if not rejected_combined.empty and "Rejection_Reason" in rejected_combined.columns:
            lines.append("REJECTION REASON BREAKDOWN")
            lines.append("-" * 70)
            counts = rejected_combined["Rejection_Reason"].value_counts()
            for reason, cnt in counts.items():
                lines.append(f"  {reason:<50} : {cnt}")
            lines.append("")

        # Column dtype summary (from last valid batch)
        lines.append("FINAL VALIDATION STATUS")
        lines.append("-" * 70)
        overall_ok = len(batches_with_issues) == 0
        status = "PASSED ✓" if overall_ok else "PASSED WITH WARNINGS ⚠"
        lines.append(f"  {status}")
        lines.append("")
        lines.append("=" * 70)

        report_text = "\n".join(lines)
        with open(VALIDATION_REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(report_text)

        print()
        print(report_text)
        print(f"\n  Validation report saved → {VALIDATION_REPORT_PATH}")
        return overall_ok
