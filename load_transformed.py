# =============================================================================
# load_transformed.py — Load cleaned data into food_orders_analytics_db
# =============================================================================
# Creates the analytics database and cleaned_food_orders table if they don't
# exist, then inserts validated records in batches using transactions.
# Each batch is appended; previously loaded batches are never overwritten.
# =============================================================================

import mysql.connector
from mysql.connector import Error
import pandas as pd
from config import ANALYTICS_DB_CONFIG, ANALYTICS_TABLE


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{ANALYTICS_TABLE}` (
    `id`                       BIGINT          AUTO_INCREMENT PRIMARY KEY,
    `Order ID`                 INT             NOT NULL,
    `Customer ID`              VARCHAR(50),
    `Restaurant ID`            VARCHAR(50),
    `Order Date and Time`      DATETIME,
    `Delivery Date and Time`   DATETIME,
    `Order Value`              DECIMAL(12, 2),
    `Delivery Fee`             DECIMAL(10, 2),
    `Payment Method`           VARCHAR(100),
    `Discounts and Offers`     VARCHAR(200),
    `Commission Fee`           DECIMAL(10, 2),
    `Payment Processing Fee`   DECIMAL(10, 2),
    `Refunds/Chargebacks`      DECIMAL(10, 2),
    `Weather Condition`        VARCHAR(100),
    `Order Channel`            VARCHAR(100),
    `Delivery_Time_Minutes`    DECIMAL(10, 2),
    `Platform_Net_Revenue`     DECIMAL(12, 2),
    UNIQUE KEY `uq_order_id` (`Order ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

INSERT_SQL = f"""
INSERT IGNORE INTO `{ANALYTICS_TABLE}` (
    `Order ID`, `Customer ID`, `Restaurant ID`,
    `Order Date and Time`, `Delivery Date and Time`,
    `Order Value`, `Delivery Fee`, `Payment Method`,
    `Discounts and Offers`, `Commission Fee`,
    `Payment Processing Fee`, `Refunds/Chargebacks`,
    `Weather Condition`, `Order Channel`,
    `Delivery_Time_Minutes`, `Platform_Net_Revenue`
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""


def _get_connection(config: dict):
    init_cfg = {k: v for k, v in config.items() if k != "database"}
    conn = mysql.connector.connect(**init_cfg)
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{config['database']}` "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    conn.commit()
    cursor.close()
    conn.close()
    return mysql.connector.connect(**config)


def setup_analytics_database():
    """Create analytics DB and cleaned_food_orders table if missing."""
    conn = _get_connection(ANALYTICS_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  Analytics DB ready: "
          f"`{ANALYTICS_DB_CONFIG['database']}`.`{ANALYTICS_TABLE}`")


def _safe_val(v):
    """Return None for NaN/NaT, else the value."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _row_tuple(row: pd.Series):
    """Convert a transformed row to an insertion tuple."""
    return (
        _safe_val(row.get("Order ID")),
        _safe_val(row.get("Customer ID")),
        _safe_val(row.get("Restaurant ID")),
        _safe_val(row.get("Order Date and Time")),
        _safe_val(row.get("Delivery Date and Time")),
        _safe_val(row.get("Order Value")),
        _safe_val(row.get("Delivery Fee")),
        _safe_val(row.get("Payment Method")),
        _safe_val(row.get("Discounts and Offers")),
        _safe_val(row.get("Commission Fee")),
        _safe_val(row.get("Payment Processing Fee")),
        _safe_val(row.get("Refunds/Chargebacks")),
        _safe_val(row.get("Weather Condition")),
        _safe_val(row.get("Order Channel")),
        _safe_val(row.get("Delivery_Time_Minutes")),
        _safe_val(row.get("Platform_Net_Revenue")),
    )


def load_transformed_batch(batch_number: int,
                            valid_df: pd.DataFrame) -> dict:
    """
    Insert one validated batch into cleaned_food_orders using a transaction.

    Parameters
    ----------
    batch_number : int
    valid_df     : transformed, validated DataFrame

    Returns
    -------
    dict with keys: success (bool), inserted (int), error (str)
    """
    if valid_df.empty:
        print(f"  Batch {batch_number:>3} | Skipped (empty DataFrame)")
        return {"success": True, "inserted": 0, "error": None}

    conn = None
    cursor = None
    try:
        conn = _get_connection(ANALYTICS_DB_CONFIG)
        cursor = conn.cursor()
        conn.start_transaction()

        rows = [_row_tuple(row) for _, row in valid_df.iterrows()]
        cursor.executemany(INSERT_SQL, rows)
        inserted = cursor.rowcount
        conn.commit()

        print(f"  Batch {batch_number:>3} | {inserted:>4} rows inserted into analytics DB ✓")
        return {"success": True, "inserted": inserted, "error": None}

    except Error as exc:
        if conn:
            conn.rollback()
        msg = f"Batch {batch_number} insertion failed: {exc}"
        print(f"  Batch {batch_number:>3} | ERROR: {exc}")
        return {"success": False, "inserted": 0, "error": msg}

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def verify_analytics_count() -> int:
    """Return the total row count of cleaned_food_orders."""
    conn = _get_connection(ANALYTICS_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{ANALYTICS_TABLE}`;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def load_all_transformed_batches(transformed_batches):
    """
    Consume transformed (valid_df, batch_number) pairs and load into analytics DB.

    Parameters
    ----------
    transformed_batches : iterable of (batch_number, valid_df)

    Returns
    -------
    dict with summary statistics
    """
    print("=" * 60)
    print("STEP 6 — Loading transformed data into food_orders_analytics_db")
    print("=" * 60)

    setup_analytics_database()
    print("-" * 60)

    total_inserted     = 0
    successful_batches = 0
    failed_batches     = 0

    for batch_number, valid_df in transformed_batches:
        result = load_transformed_batch(batch_number, valid_df)
        if result["success"]:
            total_inserted += result["inserted"]
            successful_batches += 1
        else:
            failed_batches += 1

    db_count = verify_analytics_count()
    print("-" * 60)
    print(f"  Total rows inserted  : {total_inserted}")
    print(f"  DB count verified    : {db_count}")
    print(f"  Successful batches   : {successful_batches}")
    print(f"  Failed batches       : {failed_batches}")
    print()

    return {
        "total_inserted":     total_inserted,
        "successful_batches": successful_batches,
        "failed_batches":     failed_batches,
        "db_verified_count":  db_count,
    }
