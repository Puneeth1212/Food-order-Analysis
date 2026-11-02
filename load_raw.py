# =============================================================================
# load_raw.py — Load original data into food_orders_raw_db (MySQL)
# =============================================================================
# Creates the raw database + table if they do not exist, then appends each
# batch using a database transaction.  Original values (including NULLs and
# duplicates) are preserved exactly as extracted from the CSV.
# =============================================================================

import mysql.connector
from mysql.connector import Error
import pandas as pd
from config import RAW_DB_CONFIG, RAW_TABLE, BATCH_SIZE


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{RAW_TABLE}` (
    `id`                     BIGINT        AUTO_INCREMENT PRIMARY KEY,
    `Order ID`               VARCHAR(50),
    `Customer ID`            VARCHAR(50),
    `Restaurant ID`          VARCHAR(50),
    `Order Date and Time`    VARCHAR(50),
    `Delivery Date and Time` VARCHAR(50),
    `Order Value`            VARCHAR(50),
    `Delivery Fee`           VARCHAR(50),
    `Payment Method`         VARCHAR(100),
    `Discounts and Offers`   VARCHAR(200),
    `Commission Fee`         VARCHAR(50),
    `Payment Processing Fee` VARCHAR(50),
    `Refunds/Chargebacks`    VARCHAR(50),
    `Weather Condition`      VARCHAR(100),
    `Order Channel`          VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

INSERT_SQL = f"""
INSERT INTO `{RAW_TABLE}` (
    `Order ID`, `Customer ID`, `Restaurant ID`,
    `Order Date and Time`, `Delivery Date and Time`,
    `Order Value`, `Delivery Fee`, `Payment Method`,
    `Discounts and Offers`, `Commission Fee`,
    `Payment Processing Fee`, `Refunds/Chargebacks`,
    `Weather Condition`, `Order Channel`
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""


def _get_connection(config: dict):
    """Return a MySQL connection; auto-creates the database if needed."""
    # First connect without specifying the database to create it if missing
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

    # Re-connect with the target database
    return mysql.connector.connect(**config)


def setup_raw_database():
    """Create database and raw_food_orders table if they don't exist."""
    conn = _get_connection(RAW_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  Raw DB ready: `{RAW_DB_CONFIG['database']}`.`{RAW_TABLE}`")


def _row_tuple(row: pd.Series):
    """Convert a pandas Series row to a tuple, replacing NaN with None."""
    return tuple(None if pd.isna(v) else str(v) for v in (
        row.get("Order ID"),
        row.get("Customer ID"),
        row.get("Restaurant ID"),
        row.get("Order Date and Time"),
        row.get("Delivery Date and Time"),
        row.get("Order Value"),
        row.get("Delivery Fee"),
        row.get("Payment Method"),
        row.get("Discounts and Offers"),
        row.get("Commission Fee"),
        row.get("Payment Processing Fee"),
        row.get("Refunds/Chargebacks"),
        row.get("Weather Condition"),
        row.get("Order Channel"),
    ))


def load_raw_batch(batch_number: int, batch_df: pd.DataFrame) -> bool:
    """
    Insert one batch of raw records into raw_food_orders using a transaction.

    Parameters
    ----------
    batch_number : int   — 1-based batch index (for logging)
    batch_df     : DataFrame — raw extracted records

    Returns
    -------
    bool — True on success, False on failure
    """
    conn = None
    cursor = None
    try:
        conn = _get_connection(RAW_DB_CONFIG)
        cursor = conn.cursor()
        conn.start_transaction()

        rows = [_row_tuple(row) for _, row in batch_df.iterrows()]
        cursor.executemany(INSERT_SQL, rows)
        conn.commit()

        print(f"  Batch {batch_number:>3} | {len(rows):>4} rows inserted into raw DB ✓")
        return True

    except Error as exc:
        if conn:
            conn.rollback()
        print(f"  Batch {batch_number:>3} | ERROR: {exc}")
        return False

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def verify_raw_count() -> int:
    """Return the total number of rows currently in raw_food_orders."""
    conn = _get_connection(RAW_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{RAW_TABLE}`;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def load_all_raw_batches(batch_generator):
    """
    Consume the extract generator and load every batch into the raw DB.

    Parameters
    ----------
    batch_generator : iterable of (int, DataFrame)

    Returns
    -------
    dict with keys: total_loaded, successful_batches, failed_batches
    """
    print("=" * 60)
    print("STEP 2 — Loading raw data into MySQL (food_orders_raw_db)")
    print("=" * 60)

    setup_raw_database()
    print("-" * 60)

    total_loaded       = 0
    successful_batches = 0
    failed_batches     = 0

    for batch_number, batch_df in batch_generator:
        ok = load_raw_batch(batch_number, batch_df)
        if ok:
            total_loaded += len(batch_df)
            successful_batches += 1
        else:
            failed_batches += 1

    db_count = verify_raw_count()
    print("-" * 60)
    print(f"  Total rows loaded : {total_loaded}")
    print(f"  DB count verified : {db_count}")
    print(f"  Successful batches: {successful_batches}")
    print(f"  Failed batches    : {failed_batches}")
    print()

    return {
        "total_loaded":       total_loaded,
        "successful_batches": successful_batches,
        "failed_batches":     failed_batches,
        "db_verified_count":  db_count,
    }
