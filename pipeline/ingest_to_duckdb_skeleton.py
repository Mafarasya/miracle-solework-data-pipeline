import logging
from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = Path("data/warehouse/sepokat.duckdb")
REAL_2023_PATH = Path("data/raw/Sepokat.xlsx")
REAL_2025_PATH = Path("data/raw/Sepokat-Vian.xlsx")
SYNTH_ORDERS_PATH = Path("data/synthetic/synthetic_orders.csv")
SYNTH_SUPPLIES_PATH = Path("data/synthetic/synthetic_supplies.csv")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


# ============================================================
# 1) EXTRACT
# ============================================================
def read_orders_excel(path: Path, sheet_name: str = "Pemasukan") -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name)


def read_expenses_excel(path: Path, sheet_name: str = "Pengeluaran") -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name)


def read_synthetic_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


# ============================================================
# 2) TRANSFORM LIGHT (raw normalization only)
# ============================================================
def normalize_orders_real(df: pd.DataFrame, data_source: str) -> pd.DataFrame:
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

    wanted_cols = [
        "order_no", "order_date", "customer_name", "shoe_type", "service_type",
        "price", "discount", "total_price", "notes", "payment_status", "worker"
    ]
    out = out[[c for c in wanted_cols if c in out.columns]].copy()

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


def normalize_expenses_real(df: pd.DataFrame, data_source: str) -> pd.DataFrame:
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
        "expense_date", "item_name", "brand", "unit_price", "quantity", "total_price", "notes"
    ]
    out = out[[c for c in wanted_cols if c in out.columns]].copy()

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


# ============================================================
# 3) QUALITY CHECKS
# ============================================================
def validate_orders(df: pd.DataFrame) -> None:
    pass


def validate_expenses(df: pd.DataFrame) -> None:
    pass


# ============================================================
# 4) LOAD TO DUCKDB
# ============================================================
def write_orders_table(con: duckdb.DuckDBPyConnection, df_orders: pd.DataFrame) -> None:
    con.register("df_orders", df_orders)
    con.execute("DROP TABLE IF EXISTS raw_orders")
    con.execute("""
        CREATE TABLE raw_orders AS
        SELECT
            order_no,
            TRY_CAST(order_date AS DATE)      AS order_date,
            customer_name,
            shoe_type,
            service_type,
            TRY_CAST(price AS INTEGER)        AS price,
            TRY_CAST(discount AS INTEGER)     AS discount,
            TRY_CAST(total_price AS INTEGER)  AS total_price,
            notes,
            payment_status,
            worker,
            data_source,
            CAST(is_synthetic AS BOOLEAN)     AS is_synthetic
        FROM df_orders
        ORDER BY order_date, order_no
    """)


def write_expenses_table(con: duckdb.DuckDBPyConnection, df_expenses: pd.DataFrame) -> None:
    con.register("df_expenses", df_expenses)
    con.execute("DROP TABLE IF EXISTS raw_expenses")
    con.execute("""
        CREATE TABLE raw_expenses AS
        SELECT
            TRY_CAST(expense_date AS DATE)    AS expense_date,
            item_name,
            brand,
            TRY_CAST(unit_price AS INTEGER)   AS unit_price,
            TRY_CAST(quantity AS INTEGER)     AS quantity,
            TRY_CAST(total_price AS INTEGER)  AS total_price,
            notes,
            data_source,
            CAST(is_synthetic AS BOOLEAN)     AS is_synthetic
        FROM df_expenses
        ORDER BY expense_date, item_name
    """)


# ============================================================
# 5) VERIFY
# ============================================================
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


# ============================================================
# 6) MAIN ORCHESTRATION
# ============================================================
def main():
    log.info("[1/6] Reading sources...")
    orders_2023 = read_orders_excel(REAL_2023_PATH, sheet_name="Pemasukan")
    orders_2025 = read_orders_excel(REAL_2025_PATH, sheet_name="Pemasukan")
    synth_orders = read_synthetic_csv(SYNTH_ORDERS_PATH)

    expenses_2023 = read_expenses_excel(REAL_2023_PATH, sheet_name="Pengeluaran")
    expenses_2025 = read_expenses_excel(REAL_2025_PATH, sheet_name="Pengeluaran")
    synth_expenses = read_synthetic_csv(SYNTH_SUPPLIES_PATH)

    log.info("[2/6] Normalizing orders...")
    df_orders = pd.concat([
        normalize_orders_real(orders_2023, data_source="real_2023"),
        normalize_orders_real(orders_2025, data_source="real_2025_2026"),
        normalize_orders_synth(synth_orders, data_source="synthetic_orders"),
    ], ignore_index=True)

    log.info("[3/6] Normalizing expenses...")
    df_expenses = pd.concat([
        normalize_expenses_real(expenses_2023, data_source="real_2023"),
        normalize_expenses_real(expenses_2025, data_source="real_2025_2026"),
        normalize_expenses_synth(synth_expenses, data_source="synthetic_supplies"),
    ], ignore_index=True)

    log.info("[4/6] Running validations...")
    validate_orders(df_orders)
    validate_expenses(df_expenses)

    log.info(f"[5/6] Writing DuckDB → {DB_PATH}")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    write_orders_table(con, df_orders)
    write_expenses_table(con, df_expenses)

    log.info("[6/6] Verification...")
    verify_orders(con)
    verify_expenses(con)
    con.close()

    log.info("✅ Done! raw_orders and raw_expenses loaded into DuckDB")


if __name__ == "__main__":
    main()
