# =============================================================================
# config.py — Pipeline Configuration
# =============================================================================
# Centralises all configurable settings: file paths, batch size, and database
# connection parameters. Secrets are read from a .env file (never committed).
# =============================================================================

import os
from dotenv import load_dotenv

# Load variables from the .env file in the project folder
load_dotenv()

# ---------------------------------------------------------------------------
# File Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the source CSV file (defaults to food_orders.csv next to this file;
# override with CSV_FILE_PATH in .env if the file is elsewhere)
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", os.path.join(BASE_DIR, "food_orders.csv"))

# Directory where reports will be written
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

VALIDATION_REPORT_PATH = os.path.join(REPORTS_DIR, "validation_report.txt")
REJECTED_RECORDS_PATH  = os.path.join(REPORTS_DIR, "rejected_records.csv")

# ---------------------------------------------------------------------------
# Batch Size
# ---------------------------------------------------------------------------
BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# MySQL connection (shared by both databases)
# ---------------------------------------------------------------------------
_MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
if not _MYSQL_PASSWORD:
    raise EnvironmentError(
        "MYSQL_PASSWORD is not set. Create a .env file (see .env.example) "
        "and add your MySQL password."
    )

_DB_BASE = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "port":     int(os.getenv("MYSQL_PORT", "3306")),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": _MYSQL_PASSWORD,
}

# ---------------------------------------------------------------------------
# MySQL — Raw Database
# ---------------------------------------------------------------------------
RAW_DB_CONFIG = {**_DB_BASE, "database": "food_orders_raw_db"}

RAW_TABLE = "raw_food_orders"

# ---------------------------------------------------------------------------
# MySQL — Analytics Database
# ---------------------------------------------------------------------------
ANALYTICS_DB_CONFIG = {**_DB_BASE, "database": "food_orders_analytics_db"}

ANALYTICS_TABLE = "cleaned_food_orders"

# ---------------------------------------------------------------------------
# Column names (as they appear in the CSV)
# ---------------------------------------------------------------------------
COL_ORDER_ID            = "Order ID"
COL_CUSTOMER_ID         = "Customer ID"
COL_RESTAURANT_ID       = "Restaurant ID"
COL_ORDER_DT            = "Order Date and Time"
COL_DELIVERY_DT         = "Delivery Date and Time"
COL_ORDER_VALUE         = "Order Value"
COL_DELIVERY_FEE        = "Delivery Fee"
COL_PAYMENT_METHOD      = "Payment Method"
COL_DISCOUNTS           = "Discounts and Offers"
COL_COMMISSION_FEE      = "Commission Fee"
COL_PAYMENT_PROC_FEE    = "Payment Processing Fee"
COL_REFUNDS             = "Refunds/Chargebacks"
COL_WEATHER             = "Weather Condition"
COL_ORDER_CHANNEL       = "Order Channel"

# Derived columns added during transformation
COL_DELIVERY_TIME_MINS  = "Delivery_Time_Minutes"
COL_PLATFORM_NET_REV    = "Platform_Net_Revenue"