# =============================================================================
# extract.py — Extract source data from CSV in batches
# =============================================================================
# Reads food_orders.csv in chunks of BATCH_SIZE rows without applying any
# cleaning or transformation.  The raw DataFrame chunks are yielded one at a
# time so that each downstream stage can process them incrementally.
# =============================================================================

import pandas as pd
from config import CSV_FILE_PATH, BATCH_SIZE


def extract_batches():
    """
    Generator that yields (batch_number, DataFrame) tuples by reading the
    source CSV file in chunks of BATCH_SIZE rows.

    No cleaning, deduplication, or type conversion is performed here.
    Missing values are preserved exactly as they appear in the file.

    Yields
    ------
    tuple[int, pd.DataFrame]
        batch_number  : 1-based batch index
        batch_df      : raw DataFrame slice
    """
    print("=" * 60)
    print("STEP 1 — Extracting source data from CSV")
    print("=" * 60)
    print(f"  Source file : {CSV_FILE_PATH}")
    print(f"  Batch size  : {BATCH_SIZE} rows")
    print("-" * 60)

    batch_number = 0
    total_records = 0

    for chunk in pd.read_csv(CSV_FILE_PATH, chunksize=BATCH_SIZE, dtype=str,
                              keep_default_na=True):
        batch_number += 1
        record_count = len(chunk)
        total_records += record_count

        print(f"  Batch {batch_number:>3} | Records: {record_count}")
        yield batch_number, chunk

    print("-" * 60)
    print(f"  Extraction complete — {total_records} total records "
          f"across {batch_number} batches")
    print()


def count_total_records():
    """Return the total number of rows in the CSV without loading everything."""
    df = pd.read_csv(CSV_FILE_PATH, dtype=str)
    return len(df)


if __name__ == "__main__":
    # Quick smoke-test
    all_batches = list(extract_batches())
    print(f"Extracted {sum(len(b) for _, b in all_batches)} rows total.")
