import os
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Seed
random.seed(42)
np.random.seed(42)

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR  = os.path.join(BASE_DIR, "data", "synthetic")
os.makedirs(OUT_DIR, exist_ok=True)

# Orders Config
KANG_CUCI = ["Vian", "Amory", "Rio"]

JENIS_CUCI_DIST = {
    "Deep Clean": 0.86,
    "Fast Clean": 0.08,
    "Tas":        0.03,
    "Reparasi":   0.02,
    "Karpet":     0.01,
}

HARGA_MAP = {
    "Deep Clean": lambda: random.choice([40000, 45000, 50000, 60000, 65000]),
    "Fast Clean": lambda: 35000,
    "Tas":        lambda: random.choice([30000, 80000, 90000]),
    "Reparasi":   lambda: random.choice([40000, 50000]),
    "Karpet":     lambda: random.choice([30000, 40000]),
}

SEPATU_POOL = [
    "Adidas", "Nike", "Vans", "Skechers", "Converse", "Diadora",
    "Onitsuka", "On Cloud", "NB", "Hoka", "Puma", "Warrior",
    "UnderArmour", "Compass", "Reebok", "Asics",
    "Converse High", "Converse Allstar", "Patrobas", "Wakai", "Zara", "Aerostreet",
]

NAMA_POOL = [
    "Andi", "Budi", "Citra", "Dewi", "Eko", "Fajar", "Gita", "Hana",
    "Ivan", "Julia", "Kevin", "Lisa", "Mario", "Nadia", "Oscar", "Putri",
    "Qori", "Reza", "Sari", "Tono", "Uma", "Vino", "Wulan", "Xena",
    "Yogi", "Zara", "Alif", "Bella", "Chandra", "Dina", "Erwin", "Fira",
    "Galih", "Hesti", "Imam", "Joko", "Karin", "Lukman", "Mita", "Nando",
]

# List expenses / supplies, with interval (how often these items are bought)
SUPPLIES_POOL = [
    ("Andrrows Shoe Cleaner", "Andrrows", 128800, 1, 2),   # every 2 months
    ("Plastik Sepatu",        "No name",   85000, 1, 1),   # every month
    ("Microfiber",            "Olaf",      32500, 3, 3),   # every 3 months, buy 3 pcs
    ("Silica Gel",            "General",   16650, 1, 2),
    ("Parfume Sepatu",        "Bowin",     15400, 2, 3),   # buy 2 pcs
    ("Insole Brush",          "HYPOSPOT",  20962, 1, 6),   # every 6 months
    ("Eraser Suede",          "Undersole", 37000, 1, 4),
    ("Leather Balm",          "Simplycist",31804, 1, 4),
    ("Spons",                 "Apen",      13772, 1, 2),
]

# Generator: Orders 
def generate_orders(start_date, end_date, n_orders, kang_cuci_list):
    records = []
    date_range = (end_date - start_date).days

    for _ in range(n_orders):
        tanggal    = start_date + timedelta(days=random.randint(0, date_range))
        jenis_cuci = random.choices(
            list(JENIS_CUCI_DIST.keys()),
            weights=list(JENIS_CUCI_DIST.values())
        )[0]
        harga      = HARGA_MAP[jenis_cuci]()
        discount   = random.choice([0, 0, 0, 5000, 10000])
        total      = harga - discount
        jenis_sepatu = (
            random.choice(SEPATU_POOL)
            if jenis_cuci not in ["Tas", "Karpet"]
            else jenis_cuci.title()
        )

        records.append({
            "Tanggal":          tanggal,
            "Nama":             random.choice(NAMA_POOL),
            "Jenis Sepatu":     jenis_sepatu,
            "Jenis cuci":       jenis_cuci,
            "Harga":            harga,
            "Discount":         discount,
            "Total harga":      total,
            "Keterangan":       "Synthetic",
            "Status Pembayaran":"Paid",
            "Kang Cuci":        random.choice(kang_cuci_list),
            "is_synthetic":     True,
        })

    df = (
        pd.DataFrame(records)
        .sort_values("Tanggal")
        .reset_index(drop=True)
    )
    df.insert(0, "No", range(1, len(df) + 1))
    return df


# Generator: Supplies
def generate_supplies(start_date, end_date):
    records = []

    for nama, merk, harga, jumlah, interval_months in SUPPLIES_POOL:
        current = start_date
        while current <= end_date:
            # tambah sedikit variasi hari biar tidak semua tanggal 1
            jitter      = timedelta(days=random.randint(0, 10))
            tanggal     = current + jitter
            total_harga = harga * jumlah

            records.append({
                "Tanggal":     tanggal,
                "Nama Barang": nama,
                "Merk":        merk,
                "Harga":       harga,
                "Jumlah":      jumlah,
                "Total Harga": total_harga,
                "Keterangan":  "",
                "is_synthetic": True,
            })
            current += relativedelta(months=interval_months)

    df = (
        pd.DataFrame(records)
        .sort_values("Tanggal")
        .reset_index(drop=True)
    )
    return df

# Main
if __name__ == "__main__":
    START = datetime(2025, 1, 1)
    END   = datetime(2026, 4, 14)

    print("=" * 50)
    print("  generate_synthetic.py — Sepokat Pipeline")
    print("=" * 50)

    # Orders
    print("\n[1/3] Generating orders...")
    df_orders = generate_orders(START, END, n_orders=300, kang_cuci_list=KANG_CUCI)
    path_orders = os.path.join(OUT_DIR, "synthetic_orders.csv")
    df_orders.to_csv(path_orders, index=False)
    print(f"      {len(df_orders)} rows → {path_orders}")
    print(f"      Kang Cuci: {df_orders['Kang Cuci'].value_counts().to_dict()}")

    # Supplies
    print("\n[2/3] Generating supplies expenses...")
    df_supplies = generate_supplies(START, END)
    path_supplies = os.path.join(OUT_DIR, "synthetic_supplies.csv")
    df_supplies.to_csv(path_supplies, index=False)
    print(f"      {len(df_supplies)} rows → {path_supplies}")

    print("\n✅ All synthetic data generated!")