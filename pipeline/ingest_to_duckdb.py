from distutils.log import error
import logging
import os
import re
from pathlib import Path
import duckdb
import pandas as pd

import logging
from datetime import datetime

# Paths
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "warehouse" / "miracle_solework.duckdb"
REAL_2023_PATH = BASE_DIR / "data" / "raw" / "sepokat_2023.csv"
REAL_2025_PATH = BASE_DIR / "data" / "raw" / "sepokat_2025_2026.csv"
REAL_EXPENSES_PATH_2023 = BASE_DIR / "data" / "raw" / "sepokat_spending_2023.csv"
REAL_EXPENSES_PATH_2025 = BASE_DIR / "data" / "raw" / "sepokat_spending_2025_2026.csv"
SYNTH_ORDERS_PATH = BASE_DIR / "data" / "synthetic" / "synthetic_orders.csv"
SYNTH_SUPPLIES_PATH = BASE_DIR / "data" / "synthetic" / "synthetic_supplies.csv"
LOG_DIR = BASE_DIR / "logs"

# Logging
LOG_DIR.mkdir(exist_ok=True)
log_filename = LOG_DIR / f"ingest{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

MONTH_MAP_ID = {
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "agustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "desember": 12,
}

# Helpers
def parse_indonesian_month(text):
    if pd.isna(text):
        return None

    text = str(text).strip().lower()
    return MONTH_MAP_ID.get(text)


def rp_to_int(value) -> int | None:
    """Convert Indonesian Rupiah strings like 'Rp50.000' or 'Rp67.400,00' → int.
    Returns None for NaN/unparseable values so NULLs stay explicit.
    """
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    cleaned = re.sub(r"[Rp\s]", "", str(value))  # strip 'Rp' and whitespace
    cleaned = cleaned.replace(".", "").replace(",", "")  # remove thousand-sep dots & decimal commas
    # After stripping, the last 2 digits are decimals in ',00' format → already gone
    # But if original had no decimal (e.g. 'Rp50.000') we already stripped the dot
    try:
        val = int(cleaned)
        # If original had ',00' suffix the result is 100x too large — divide back
        # Detect: original string contained a comma (cents separator)
        if "," in str(value):
            val = val // 100
        return val
    except ValueError:
        return None


def inspect_dataframe(df: pd.DataFrame, name: str) -> None:
    """Print a diagnostic snapshot of a DataFrame before ingest."""
    sep = "-" * 60
    log.info(f"\n{sep}")
    log.info(f"[INSPECT] {name}  shape={df.shape}")
    log.info(f"{sep}")
    log.info("dtypes:\n" + df.dtypes.to_string())
    log.info("\nnull counts:\n" + df.isnull().sum().to_string())
    log.info("\nsample (head 5):\n" + df.head(5).to_string(index=False))
    # Highlight numeric price cols: show min/max/mean where applicable
    price_cols = [c for c in df.columns if "price" in c or "harga" in c.lower()]
    if price_cols:
        log.info(f"\nprice col stats: {price_cols}")
        log.info(df[price_cols].describe().to_string())
    log.info(sep)

# def impute_expense_date(row, default_year=2023, salary_day=25):
#     raw_date = pd.to_datetime(row.get("expense_date"), errors="coerce")
#     if pd.notna(raw_date):
#         return raw_date

#     brand = str(row.get("brand", "")).strip().lower()
#     month_num = parse_indonesian_month(row.get("notes"))

#     if brand == "gaji" and month_num:
#         return pd.Timestamp(year=default_year, month=month_num, day=salary_day)

#     return pd.NaT

def is_month(value) -> bool:
    if pd.isna(value):
        return False
    return str(value).strip().lower() in MONTH_MAP_ID
    


# Extract from excel
def read_orders_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def read_expenses_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def read_synthetic_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# Light Transform - Raw Normalization
def normalize_orders_real(df: pd.DataFrame, data_source: str) -> pd.DataFrame:
    out = df.copy()

    rename_map = {
    "No":                 "order_no",
    "Tanggal":            "order_date",
    "Nama":               "customer_name",
    "Jenis Sepatu":       "shoe_type",
    "Jenis cuci":         "service_type",
    "Harga":              "price",
    "Discount":           "discount",
    "Total harga":        "total_price",
    "Keterangan":         "notes",
    "Status Pembayaran":  "payment_status",
    "Kang Cuci":          "worker",
    }
    
    out = out.rename(columns=rename_map)

    wanted_cols = [
        "order_no",
        "order_date",
        "customer_name",
        "shoe_type",
        "service_type",
        "price",
        "discount",
        "total_price",
        "notes",
        "payment_status",
        "worker"
    ]

    out = out[wanted_cols].copy()

    out["order_date"] = pd.to_datetime(out["order_date"], dayfirst=True, errors='coerce')

    # Clean Rp-formatted price columns → plain integers
    for col in ["price", "discount", "total_price"]:
        out[col] = out[col].apply(rp_to_int)

    out["data_source"] = data_source
    out["is_synthetic"] = False

    return out

def normalize_orders_synth(df: pd.DataFrame, data_source: str = "synthetic_orders") -> pd.DataFrame:
    out = df.copy()

    rename_map = {
        "No": "order_no",
        "Tanggal": "order_date",
        "Nama": "customer_name",
        "Jenis Sepatu": "shoe_type",
        "Jenis cuci": "service_type",
        "Harga": "price",
        "Discount": "discount",
        "Total harga": "total_price",
        "Keterangan": "notes",
        "Status Pembayaran": "payment_status",
        "Kang Cuci": "worker",
    }

    out = out.rename(columns=rename_map)
    out["data_source"] = data_source
    out["is_synthetic"] = True
    return out

def normalize_expenses_real(df: pd.DataFrame, data_source: str, year: int) -> pd.DataFrame:
    out = df.copy()

    rename_map = {
        "Tanggal": "expense_date",
        "Nama Barang": "item_name",
        "Merk": "brand",
        "Harga": "unit_price",
        "Jumlah": "quantity",
        "Total Harga": "total_price",
        "Keterangan": "notes",
    }

    out = out.rename(columns=rename_map)

    wanted_cols = [
        "expense_date",
        "item_name",
        "brand",
        "unit_price",
        "quantity",
        "total_price",
        "notes",
    ]
    out = out[wanted_cols].copy()

    # Parse raw date first
    raw_dates = pd.to_datetime(out["expense_date"], errors="coerce")

  
    return out

def normalize_expenses_real(df: pd.DataFrame, data_source: str, year: int) -> pd.DataFrame:
    out = df.copy()

    rename_map = {
        "Tanggal": "expense_date",
        "Nama Barang": "item_name",
        "Merk": "brand",
        "Harga": "unit_price",
        "Jumlah": "quantity",
        "Total Harga": "total_price",
        "Keterangan": "notes",
    }

    out = out.rename(columns=rename_map)

    wanted_cols = [
        "expense_date",
        "item_name",
        "brand",
        "unit_price",
        "quantity",
        "total_price",
        "notes",
    ]
    out = out[wanted_cols].copy()

    # Parse raw date first
    raw_dates = pd.to_datetime(out["expense_date"], errors="coerce")

    # Backward scan:
    # every row inherits month from the next salary row below it
    next_month = None
    month_filled = [None] * len(out)

    for i in range(len(out) - 1, -1, -1):
        brand = str(out.iloc[i]["brand"]).strip().lower()
        notes = out.iloc[i]["notes"]

        if brand == "gaji" and is_month(notes):
            next_month = MONTH_MAP_ID[str(notes).strip().lower()]

        month_filled[i] = next_month

    # Final expense_date:
    out["expense_date"] = [
        raw_dates.iloc[i]
        if pd.notna(raw_dates.iloc[i])
        else pd.Timestamp(year=year, month=month_num, day=25)
        if month_num
        else pd.NaT
        for i, month_num in enumerate(month_filled)
    ]

    out["is_date_imputed"] = raw_dates.isna() & out["expense_date"].notna()

    # Clean numeric columns
    for col in ["unit_price", "total_price"]:
        out[col] = out[col].apply(rp_to_int)

    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")

    out["data_source"] = data_source
    out["is_synthetic"] = False

    return out 

def normalize_expenses_synth(df: pd.DataFrame, data_source: str = "synthetic_supplies") -> pd.DataFrame:
    out = df.copy()

    rename_map = {
        "Tanggal": "expense_date",
        "Nama Barang": "item_name",
        "Merk": "brand",
        "Harga": "unit_price",
        "Jumlah": "quantity",
        "Total Harga": "total_price",
        "Keterangan": "notes",
    }

    out = out.rename(columns=rename_map)
    out["data_source"] = data_source
    out["is_synthetic"] = True
    return out

# Quality Checks
def validate_orders(df: pd.DataFrame) -> None:
    required_cols = ["order_no", "order_date", "service_type", "total_price", "worker"]
    
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    parsed_dates = pd.to_datetime(df["order_date"], errors="coerce")
    bad_date_count = parsed_dates.isna().sum()
    if bad_date_count > 0:
        raise ValueError(f"{bad_date_count} rows have unparseable order_date")

    if(pd.to_numeric(df["total_price"], errors="coerce") < 0).any():
        raise ValueError("Found negative total_price")

    worker_null_rate = df["worker"].isna().mean()
    if worker_null_rate > 0.3:
        raise ValueError(f"Worker null rate {worker_null_rate:.2%} is too high")

def validate_expenses(df: pd.DataFrame) -> None:
    # check item_name, quantity total_price
    required_cols = ["item_name", "quantity", "total_price"]

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    # quantity
    if(pd.to_numeric(df["quantity"], errors="coerce") < 0).any():
        raise ValueError("Found negative quantity")

    # total_price
    if(pd.to_numeric(df["total_price"], errors="coerce") < 0).any():
        raise ValueError("Found negative total_price")

    item_name_nullrate = df["item_name"].isna().mean()
    if item_name_nullrate > 0.3:
        raise ValueError(f"Item name null rate {item_name_nullrate:.2%} is too high")
        

# Load to DB
def write_orders_table(con: duckdb.DuckDBPyConnection, df_orders: pd.DataFrame) -> None:
    con.register("df_orders", df_orders)
    con.execute("DROP TABLE IF EXISTS raw_orders")
    con.execute("""
        CREATE TABLE raw_orders AS
        SELECT
            order_no,
            TRY_CAST(order_date as DATE) AS order_date,
            customer_name,
            shoe_type,
            service_type,
            TRY_CAST(price AS INTEGER) AS price,
            TRY_CAST(discount AS INTEGER) AS discount,
            TRY_CAST(total_price AS INTEGER) AS total_price,
            notes,
            payment_status,
            worker,
            data_source,
            CAST(is_synthetic AS BOOLEAN) AS is_synthetic
        FROM df_orders
        ORDER BY order_date, order_no
    """)

def write_expenses_table(con: duckdb.DuckDBPyConnection, df_expenses: pd.DataFrame) -> None:
    con.register("df_expenses", df_expenses)
    con.execute("DROP TABLE IF EXISTS raw_expenses")
    con.execute("""
        CREATE TABLE raw_expenses AS
        SELECT
            TRY_CAST(expense_date AS DATE) AS expense_date,
            item_name,
            brand,
            TRY_CAST(unit_price AS INTEGER) AS unit_price,
            TRY_CAST(quantity AS INTEGER) AS quantity,
            TRY_CAST(total_price AS INTEGER) AS total_price,
            notes,
            data_source,
            CAST(is_synthetic AS BOOLEAN) AS is_synthetic,
            CAST(is_date_imputed AS BOOLEAN) AS is_date_imputed
        FROM df_expenses
        ORDER BY expense_date, item_name
    """)

# Verify
def verify_orders(con: duckdb.DuckDBPyConnection) -> None:
    log.info("\n[VERIFY] raw_orders")
    result = con.execute("""
        SELECT
            data_source,
            COUNT(*) AS row_count,
            MIN(order_date) AS earliest,
            MAX(order_date) AS latest,
            AVG(total_price) AS avg_total_price
        FROM raw_orders
        GROUP BY data_source
        ORDER BY data_source
    """).fetchdf()
    log.info(result.to_string(index=False))


def verify_expenses(con: duckdb.DuckDBPyConnection) -> None:
    log.info("\n[VERIFY] raw_expenses")
    result = con.execute("""
        SELECT
            data_source,
            COUNT(*) AS row_count,
            MIN(expense_date) AS earliest,
            MAX(expense_date) AS latest,
            AVG(total_price) AS avg_total_price
        FROM raw_expenses
        GROUP BY data_source
        ORDER BY data_source
    """).fetchdf()
    log.info(result.to_string(index=False))

# Orchestrate
def main():
    log.info("[1/6 Reading sources...]")
    orders_2023 = read_orders_csv(REAL_2023_PATH)
    orders_2025 = read_orders_csv(REAL_2025_PATH)
    synth_orders = read_synthetic_csv(SYNTH_ORDERS_PATH)

    expenses_2023 = read_expenses_csv(REAL_EXPENSES_PATH_2023)
    expenses_2025 = read_expenses_csv(REAL_EXPENSES_PATH_2025)
    synth_expenses = read_synthetic_csv(SYNTH_SUPPLIES_PATH)

    log.info("[2/6 Normalizing orders...]")
    # Merge between real & synthetic data
    df_orders = pd.concat([
        normalize_orders_real(orders_2023, data_source="real_2023"),
        normalize_orders_real(orders_2025, data_source="real_2025"),
        normalize_orders_synth(synth_orders, data_source="synthetic_orders")
    ], ignore_index=True)

    log.info("[3/6 Normalizing expenses...]")
    # Merge between real & synthetic data
    df_expenses = pd.concat([
        normalize_expenses_real(expenses_2023, data_source="real_2023", year=2023),
        normalize_expenses_real(expenses_2025, data_source="real_2025", year=2025),
        normalize_expenses_synth(synth_expenses, data_source="synthetic_supplies")
    ], ignore_index=True)

    # ── Pre-ingest inspection ──────────────────────────────────────────
    log.info("[4/6 Running Validations & Pre-ingest Inspection...]")
    inspect_dataframe(df_orders, "df_orders (merged)")
    inspect_dataframe(df_expenses, "df_expenses (merged)")
    validate_orders(df_orders)
    validate_expenses(df_expenses)

    log.info("[5/6] Writing to DuckDB -> {DB_PATH}")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    write_orders_table(con, df_orders)
    write_expenses_table(con, df_expenses)

    log.info("[6/6] Verifying tables...")
    verify_orders(con)
    verify_expenses(con)
    con.close()

    log.info("Donee :D raw_orders and raw_expenses loaded into DuckDatabase")

if __name__ == "__main__":
    main()