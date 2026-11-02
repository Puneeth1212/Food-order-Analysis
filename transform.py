# =============================================================================
# transform.py — Clean and transform raw DataFrames
# =============================================================================
#
# Transformation decisions (documented):
#
# 1. DUPLICATE REMOVAL
#    Key  : "Order ID" — an order is uniquely identified by its Order ID.
#    Cross-batch deduplication: a global set of seen Order IDs is maintained
#    in module-level state so duplicates that span batch boundaries are caught.
#
# 2. MISSING VALUE HANDLING
#    Order Value          → median of the column (robust to outliers)
#    Delivery Fee         → 0.0  (free delivery assumed when not recorded)
#    Commission Fee       → median of the column
#    Payment Proc. Fee    → median of the column
#    Refunds/Chargebacks  → 0.0  (no refund assumed when not recorded)
#    Payment Method       → "Unknown"
#    Discounts and Offers → "None"
#
# 3. TYPE CONVERSIONS
#    Order / Delivery timestamps → pd.Timestamp (coerce errors → NaT)
#    Financial columns           → float64  (coerce errors → NaN)
#
# 4. DERIVED COLUMNS
#    Delivery_Time_Minutes  = (Delivery DT − Order DT).total_seconds() / 60
#    Platform_Net_Revenue   = Commission Fee + Delivery Fee
#                             − Payment Processing Fee − Refunds/Chargebacks
#
# 5. INVALID RECORD REJECTION
#    • NaT in Order Date and Time or Delivery Date and Time (unparseable)
#    • Delivery_Time_Minutes < 0  (delivery before order — data error)
#    • Remaining NaN in any financial column after imputation
# =============================================================================

import pandas as pd
import numpy as np
from config import (
    COL_ORDER_ID, COL_CUSTOMER_ID, COL_RESTAURANT_ID,
    COL_ORDER_DT, COL_DELIVERY_DT, COL_ORDER_VALUE,
    COL_DELIVERY_FEE, COL_PAYMENT_METHOD, COL_DISCOUNTS,
    COL_COMMISSION_FEE, COL_PAYMENT_PROC_FEE, COL_REFUNDS,
    COL_WEATHER, COL_ORDER_CHANNEL,
    COL_DELIVERY_TIME_MINS, COL_PLATFORM_NET_REV,
)

# ---------------------------------------------------------------------------
# Cross-batch duplicate guard
# ---------------------------------------------------------------------------
_seen_order_ids: set = set()


def reset_seen_order_ids():
    """Call this before a new pipeline run to clear cross-batch memory."""
    global _seen_order_ids
    _seen_order_ids = set()


# ---------------------------------------------------------------------------
# Reference medians (computed lazily on first call, from the full CSV)
# ---------------------------------------------------------------------------
_MEDIANS: dict = {}


def _compute_medians(batch_df: pd.DataFrame):
    """
    Use the batch itself to derive fallback medians for numeric columns.
    This is used only when _MEDIANS hasn't been pre-loaded from the full CSV.
    """
    for col in [COL_ORDER_VALUE, COL_COMMISSION_FEE, COL_PAYMENT_PROC_FEE]:
        numeric = pd.to_numeric(batch_df[col], errors="coerce")
        median_val = numeric.median()
        if col not in _MEDIANS or pd.isna(_MEDIANS[col]):
            _MEDIANS[col] = median_val if not pd.isna(median_val) else 0.0


def set_global_medians(full_df: pd.DataFrame):
    """Pre-compute medians from the full dataset for consistent imputation."""
    for col in [COL_ORDER_VALUE, COL_COMMISSION_FEE, COL_PAYMENT_PROC_FEE]:
        numeric = pd.to_numeric(full_df[col], errors="coerce")
        _MEDIANS[col] = numeric.median()
    print(f"  Global medians computed: {_MEDIANS}")


# ---------------------------------------------------------------------------
# Main transform function
# ---------------------------------------------------------------------------
def transform_batch(batch_number: int, batch_df: pd.DataFrame):
    """
    Apply all transformation steps to a single raw batch.

    Parameters
    ----------
    batch_number : int
    batch_df     : raw DataFrame as retrieved from MySQL

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        valid_df    : cleaned, transformed records
        rejected_df : records that failed validation, with a Rejection_Reason
    """
    df = batch_df.copy()
    rejected_rows = []

    # ------------------------------------------------------------------
    # 0. Ensure medians are available
    # ------------------------------------------------------------------
    if not _MEDIANS:
        _compute_medians(df)

    # ------------------------------------------------------------------
    # 1. Intra-batch duplicate removal  (keep first occurrence)
    # ------------------------------------------------------------------
    before = len(df)
    df.drop_duplicates(subset=[COL_ORDER_ID], keep="first", inplace=True)
    after = len(df)
    if before != after:
        print(f"    [Transform] Batch {batch_number}: dropped {before - after} "
              f"intra-batch duplicate(s)")

    # ------------------------------------------------------------------
    # 2. Cross-batch duplicate removal
    # ------------------------------------------------------------------
    global _seen_order_ids
    cross_dup_mask = df[COL_ORDER_ID].isin(_seen_order_ids)
    if cross_dup_mask.any():
        n = cross_dup_mask.sum()
        dup_rows = df[cross_dup_mask].copy()
        dup_rows["Rejection_Reason"] = "Cross-batch duplicate Order ID"
        rejected_rows.append(dup_rows)
        df = df[~cross_dup_mask].copy()
        print(f"    [Transform] Batch {batch_number}: rejected {n} "
              f"cross-batch duplicate(s)")

    # Update global seen set
    _seen_order_ids.update(df[COL_ORDER_ID].astype(str).tolist())

    # ------------------------------------------------------------------
    # 3. Convert financial columns to numeric
    # ------------------------------------------------------------------
    financial_cols = [
        COL_ORDER_VALUE, COL_DELIVERY_FEE, COL_COMMISSION_FEE,
        COL_PAYMENT_PROC_FEE, COL_REFUNDS,
    ]
    for col in financial_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------------------------------------------
    # 4. Handle missing values
    # ------------------------------------------------------------------
    df[COL_ORDER_VALUE]      = df[COL_ORDER_VALUE].fillna(_MEDIANS.get(COL_ORDER_VALUE, 0.0))
    df[COL_DELIVERY_FEE]     = df[COL_DELIVERY_FEE].fillna(0.0)
    df[COL_COMMISSION_FEE]   = df[COL_COMMISSION_FEE].fillna(_MEDIANS.get(COL_COMMISSION_FEE, 0.0))
    df[COL_PAYMENT_PROC_FEE] = df[COL_PAYMENT_PROC_FEE].fillna(_MEDIANS.get(COL_PAYMENT_PROC_FEE, 0.0))
    df[COL_REFUNDS]          = df[COL_REFUNDS].fillna(0.0)
    df[COL_PAYMENT_METHOD]   = df[COL_PAYMENT_METHOD].fillna("Unknown")
    df[COL_DISCOUNTS]        = df[COL_DISCOUNTS].fillna("None")

    # ------------------------------------------------------------------
    # 5. Convert timestamps
    # ------------------------------------------------------------------
    df[COL_ORDER_DT]    = pd.to_datetime(df[COL_ORDER_DT],    errors="coerce")
    df[COL_DELIVERY_DT] = pd.to_datetime(df[COL_DELIVERY_DT], errors="coerce")

    # ------------------------------------------------------------------
    # 6. Reject records with unparseable timestamps
    # ------------------------------------------------------------------
    bad_ts_mask = df[COL_ORDER_DT].isna() | df[COL_DELIVERY_DT].isna()
    if bad_ts_mask.any():
        bad_ts_rows = df[bad_ts_mask].copy()
        bad_ts_rows["Rejection_Reason"] = "Invalid or missing timestamp"
        rejected_rows.append(bad_ts_rows)
        df = df[~bad_ts_mask].copy()

    # ------------------------------------------------------------------
    # 7. Derived columns
    # ------------------------------------------------------------------
    df[COL_DELIVERY_TIME_MINS] = (
        (df[COL_DELIVERY_DT] - df[COL_ORDER_DT]).dt.total_seconds() / 60
    ).round(2)

    df[COL_PLATFORM_NET_REV] = (
        df[COL_COMMISSION_FEE]
        + df[COL_DELIVERY_FEE]
        - df[COL_PAYMENT_PROC_FEE]
        - df[COL_REFUNDS]
    ).round(2)

    # ------------------------------------------------------------------
    # 8. Reject records with negative delivery time
    # ------------------------------------------------------------------
    neg_time_mask = df[COL_DELIVERY_TIME_MINS] < 0
    if neg_time_mask.any():
        neg_rows = df[neg_time_mask].copy()
        neg_rows["Rejection_Reason"] = "Negative delivery duration"
        rejected_rows.append(neg_rows)
        df = df[~neg_time_mask].copy()

    # ------------------------------------------------------------------
    # 9. Reject records where financial columns are still NaN
    # ------------------------------------------------------------------
    fin_null_mask = df[financial_cols].isna().any(axis=1)
    if fin_null_mask.any():
        fin_null_rows = df[fin_null_mask].copy()
        fin_null_rows["Rejection_Reason"] = "Remaining NaN in financial columns after imputation"
        rejected_rows.append(fin_null_rows)
        df = df[~fin_null_mask].copy()

    # ------------------------------------------------------------------
    # 10. Assemble rejected DataFrame
    # ------------------------------------------------------------------
    if rejected_rows:
        rejected_df = pd.concat(rejected_rows, ignore_index=True)
        if "Rejection_Reason" not in rejected_df.columns:
            rejected_df["Rejection_Reason"] = "Unknown"
    else:
        rejected_df = pd.DataFrame(columns=list(df.columns) + ["Rejection_Reason"])

    return df, rejected_df
