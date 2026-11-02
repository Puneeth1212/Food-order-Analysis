# =============================================================================
# retrieve_raw.py — Retrieve raw records from food_orders_raw_db in batches
# =============================================================================
# Reads raw_food_orders using a server-side cursor so that we never pull the
# entire table into memory at once.  Each batch is yielded as a Pandas
# DataFrame.  The raw table is never modified.
# =============================================================================

import math
import mysql.connector
import pandas as pd
from config import RAW_DB_CONFIG, RAW_TABLE, BATCH_SIZE


def _get_connection(config: dict):
    return mysql.connector.connect(**config)


def get_raw_row_count() -> int:
    """Return the total number of rows in raw_food_orders."""
    conn = _get_connection(RAW_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{RAW_TABLE}`;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def retrieve_raw_batches():
    """
    Generator that yields (batch_number, DataFrame) by fetching rows from
    raw_food_orders in pages of BATCH_SIZE using LIMIT / OFFSET.

    The raw table is read-only in this step; no rows are modified.

    Yields
    ------
    tuple[int, pd.DataFrame]
        batch_number : 1-based index
        batch_df     : DataFrame containing the fetched rows (columns match
                       the CSV, the auto-increment `id` column is excluded)
    """
    print("=" * 60)
    print("STEP 3 — Retrieving raw data from MySQL")
    print("=" * 60)
    print(f"  Source : `{RAW_DB_CONFIG['database']}`.`{RAW_TABLE}`")
    print(f"  Batch size : {BATCH_SIZE} rows")
    print("-" * 60)

    total_rows   = get_raw_row_count()
    total_batches = math.ceil(total_rows / BATCH_SIZE)
    print(f"  Total rows in raw DB : {total_rows}")
    print(f"  Expected batches     : {total_batches}")
    print()

    columns = [
        "Order ID", "Customer ID", "Restaurant ID",
        "Order Date and Time", "Delivery Date and Time",
        "Order Value", "Delivery Fee", "Payment Method",
        "Discounts and Offers", "Commission Fee",
        "Payment Processing Fee", "Refunds/Chargebacks",
        "Weather Condition", "Order Channel",
    ]

    col_select = ", ".join(f"`{c}`" for c in columns)
    conn = _get_connection(RAW_DB_CONFIG)
    cursor = conn.cursor()

    batch_number  = 0
    total_retrieved = 0
    offset = 0

    while True:
        query = (
            f"SELECT {col_select} FROM `{RAW_TABLE}` "
            f"ORDER BY id LIMIT %s OFFSET %s;"
        )
        cursor.execute(query, (BATCH_SIZE, offset))
        rows = cursor.fetchall()

        if not rows:
            break

        batch_number += 1
        batch_df = pd.DataFrame(rows, columns=columns)
        record_count = len(batch_df)
        total_retrieved += record_count

        print(f"  Batch {batch_number:>3} | Records retrieved: {record_count}")
        yield batch_number, batch_df

        offset += BATCH_SIZE

    cursor.close()
    conn.close()

    print("-" * 60)
    print(f"  Retrieval complete — {total_retrieved} records across "
          f"{batch_number} batches")
    print()

    return total_retrieved   # accessible via StopIteration.value


if __name__ == "__main__":
    for bn, df in retrieve_raw_batches():
        pass   # batches printed inside the generator
