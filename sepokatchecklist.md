# Sepokat — Data & Pipeline Checklist
> Dokumen referensi sebelum mulai coding. Update checklist ini seiring progress project.

***

## 1. Data Sources

| Source | File | Sheet | Baris Valid | Periode | Status |
|--------|------|-------|-------------|---------|--------|
| Orders 2023 | `Sepokat.xlsx` | Pemasukan | ~115 | Jun–Des 2023 | ✅ Ada |
| Expenses 2023 | `Sepokat.xlsx` | Pengeluaran | ~40 items | Jun–Des 2023 | ✅ Ada |
| Orders 2025–2026 | `Sepokat-Vian.xlsx` | Pemasukan | ~45 | Sep 2025–Feb 2026 | ✅ Ada |
| Expenses 2025–2026 | `Sepokat-Vian.xlsx` | Pengeluaran | ~15 items | 2025–2026 | ✅ Ada |

**Catatan penting:**
- 2023: ada 3 kang cuci → Vian, Rio, Amory
- 2025–2026: hanya 1 kang cuci → Vian
- Row 115 di 2023 (`Zefa`) punya `Total harga = 0` dan `Status Pembayaran` kosong → exclude dari analisis
- Row 46–47 di 2025–2026 (`Mas Yos`) → sudah diupdate, pastikan re-check sebelum ingest
- Ada duplikat No urut di 2023 (dua row bernomor `62`) → perlu di-handle saat assign surrogate key

***

## 2. Kolom yang Sudah Ada (Raw)

| Kolom Raw | Tipe | Catatan |
|-----------|------|---------|
| `No` | Integer | Tidak bisa dijadikan PK — ada duplikat (row 62 di 2023) |
| `Tanggal` | Date | Format `DD/MM/YYYY` di 2025 file, sudah datetime di 2023 file |
| `Nama` | String | Tidak konsisten — perlu standardize |
| `Jenis Sepatu` | String | Campur brand + material + item non-sepatu (Tas, Karpet, Sendal) |
| `Jenis cuci` | String | Perlu standardize: Deep Clean, Fast Clean, Reparasi, Tas, Karpet |
| `Harga` | Integer | List price sebelum diskon |
| `Discount` | Integer | Bisa negatif (row 44 di 2023: `-20000` = harga lebih mahal) |
| `Total harga` | Integer | = Harga - Discount |
| `Keterangan` | String | Campur: payment method (BCA, BLU) + catatan kosong |
| `Status Pembayaran` | String | Mostly "Paid", ada yang kosong |
| `Kang Cuci` | String | Vian / Rio / Amory |

***

## 3. Kolom yang Perlu Di-Derive (Staging Layer)

### 3a. Dari kolom yang sudah ada

| Kolom Baru | Sumber | Logic | Prioritas |
|------------|--------|-------|-----------|
| `order_id` | `No` + `source_year` | Surrogate key: `{year}_{no}` padded, handle duplikat | 🔴 Wajib |
| `source_year` | Nama file / `Tanggal` | `2023` atau `2025`/`2026` | 🔴 Wajib |
| `order_date` | `Tanggal` | Parse ke DATE, handle dua format | 🔴 Wajib |
| `year` | `order_date` | `EXTRACT(YEAR FROM order_date)` | 🔴 Wajib |
| `month` | `order_date` | `EXTRACT(MONTH FROM order_date)` | 🔴 Wajib |
| `week` | `order_date` | `DATE_TRUNC('week', order_date)` | 🟡 Nice |
| `customer_name_clean` | `Nama` | CASE WHEN mapping (lihat section 4) | 🔴 Wajib |
| `payment_method` | `Keterangan` | Extract: BCA/BLU → "Transfer", kosong → "Unknown" | 🟡 Nice |
| `service_category` | `Jenis cuci` + `Jenis Sepatu` | Normalize: Deep Clean / Fast Clean / Reparasi / Tas / Karpet / Sendal | 🔴 Wajib |
| `item_type` | `Jenis Sepatu` | Shoe / Bag / Carpet / Sandal / Other | 🔴 Wajib |
| `shoe_brand_clean` | `Jenis Sepatu` | Standardize brand (lihat section 5) | 🔴 Wajib |
| `shoe_material` | `shoe_brand_clean` | Canvas / Mesh / Suede / Leather / Unknown | 🟡 Nice |
| `material_complexity` | `shoe_material` | Low / Medium / High | 🟡 Nice |
| `discount_type` | `Discount` + order history | loyalty / special / none (lihat section 6) | 🔴 Wajib |

### 3b. Perlu window function / aggregasi

| Kolom Baru | Logic | Catatan |
|------------|-------|---------|
| `order_sequence_per_customer` | `ROW_NUMBER() OVER (PARTITION BY customer_name_clean, year ORDER BY order_date)` | Reset setiap tahun ganti |
| `is_repeat_customer` | `order_sequence_per_customer > 1` | True jika bukan order pertama |
| `expected_discount` | `CASE WHEN order_sequence_per_customer % 2 = 0 THEN 10000 ELSE 0 END` | Berdasarkan loyalty rule |
| `discount_gap` | `expected_discount - discount` | Jika positif → harusnya dapat diskon tapi tidak |
| `is_discount_missed` | `discount_gap > 0` | Flag anomali loyalty |

***

## 4. Customer Name Standardization (SQL CASE WHEN)

Semua mapping ini masuk di `stg_orders.sql`, bukan di raw data.

```sql
customer_name_clean = CASE
    WHEN nama = 'Kost no 12'       THEN 'Pelanggan Kost 12'
    WHEN nama = 'Temen Putri'      THEN 'Teman Putri'
    WHEN nama = 'Jebit'            THEN 'Jebit'          -- sudah konsisten, no action
    WHEN nama LIKE 'Ka %'          THEN nama             -- Ka Indra, Ka Titi, dll — keep
    WHEN nama LIKE 'Kak %'         THEN nama             -- Kak Anis, Kak Nanda, dll — keep
    WHEN nama LIKE 'Mas %'         THEN nama             -- Mas Ian, Mas Jo, dll — keep
    WHEN nama LIKE 'Ko %'          THEN nama             -- Ko David, Ko Handy — keep
    WHEN nama LIKE 'Pak %'         THEN nama             -- Pak Amir — keep
    WHEN nama LIKE 'Bang %'        THEN nama             -- Bang Adi — keep
    WHEN nama LIKE 'Mama %'        THEN nama             -- Mama Danen — keep
    ELSE nama
END
```

> **Catatan:** Tambah mapping lain saat ditemukan inkonsistensi baru saat EDA.

***

## 5. Brand Standardization (SQL CASE WHEN)

```sql
shoe_brand_clean = CASE
    WHEN LOWER(jenis_sepatu) IN ('on cloud', 'cloud', 'cloudtech', 'cloud tec') THEN 'On Running'
    WHEN LOWER(jenis_sepatu) LIKE '%nike aj%'     THEN 'Nike'
    WHEN LOWER(jenis_sepatu) = 'nike airmax'      THEN 'Nike'
    WHEN LOWER(jenis_sepatu) = 'nike af1'         THEN 'Nike'
    WHEN LOWER(jenis_sepatu) LIKE 'adidas%'       THEN 'Adidas'
    WHEN LOWER(jenis_sepatu) = 'nb'               THEN 'New Balance'
    WHEN LOWER(jenis_sepatu) = 'new balance'      THEN 'New Balance'
    WHEN LOWER(jenis_sepatu) = 'converse allstar'  THEN 'Converse'
    WHEN LOWER(jenis_sepatu) = 'converse high'    THEN 'Converse'
    WHEN LOWER(jenis_sepatu) IN ('gatau', 'adidas (lupa)', '???', 'lokal') THEN 'Unknown'
    WHEN jenis_sepatu IS NULL OR jenis_sepatu = '' THEN 'Unknown'
    ELSE INITCAP(jenis_sepatu)  -- capitalize as-is
END
```

***

## 6. Discount Type Logic

### Business Rules (dari obrolan dengan owner)
- `Rp10.000` → **loyalty**: setiap order genap per customer per tahun (ke-2, ke-4, ke-6, dst)
- `Rp5.000` → **special**: customer spesial (1 customer spesifik)
- `Rp0` → **none**
- `Rp8.000` → muncul di Aug 2023, kemungkinan promo sementara → tandai **promo**
- `Rp20.000+` → di luar range normal → **manual/special**, perlu dicek
- Discount negatif (e.g. `-20000` di row 44 2023) → **surcharge**, bukan diskon

### SQL Implementation
```sql
discount_type = CASE
    WHEN discount < 0                          THEN 'surcharge'
    WHEN discount = 0                          THEN 'none'
    WHEN discount = 5000                       THEN 'special'
    WHEN discount = 8000                       THEN 'promo'
    WHEN discount = 10000                      THEN 'loyalty'  -- perlu divalidasi vs order_sequence
    WHEN discount > 10000                      THEN 'manual'
    ELSE 'unknown'
END
```

***

## 7. Messy Data yang Perlu Di-Handle

| Issue | Lokasi | Action |
|-------|--------|--------|
| Duplikat No urut (dua row = 62) | 2023, Agustus | Generate surrogate key, jangan pakai `No` sebagai PK |
| Row kosong / placeholder | Akhir kedua sheet | Filter: `WHERE nama IS NOT NULL AND total_harga > 0` |
| Row summary (Jumlah pemasukan, Jumlah kas masuk) | Kolom kanan spreadsheet | Sudah di-skip oleh parser — bukan bagian tabel orders |
| `Jenis Sepatu` = item non-sepatu (Tas, Karpet, Sendal, Reparasi) | 2023 | Pisah ke kolom `item_type`, jangan di-exclude — tetap valid order |
| `Status Pembayaran` kosong | Row 115 (Zefa), Mas Yos awal | Tandai sebagai `pending` atau `unknown`, exclude dari revenue analysis sampai ada update |
| `Total harga = 0` | Row 115 (Zefa) | Exclude dari revenue, include di order count dengan flag |
| Discount = `50000` (= Harga penuh) di row 84 Angelica 2023 | 2023 | Tandai sebagai `free_order` — mungkin goodwill |
| `Jenis cuci = Reparasi` | 2023 beberapa row | Service type berbeda, harga berbeda — handle di `service_category` |
| Payment method `BLU` | Row 113 (Zidane 2023) | Transfer via BLU, sama dengan BCA — normalize ke "Transfer" |

***

## 8. dbt Tests yang Perlu Dibuat

| Test | Kolom | Rule |
|------|-------|------|
| `not_null` | `order_id`, `order_date`, `customer_name_clean`, `net_revenue` | Semua kolom kunci tidak boleh null |
| `unique` | `order_id` | Setiap order harus unique |
| `accepted_values` | `service_category` | Hanya: Deep Clean, Fast Clean, Reparasi, Tas, Karpet, Sendal |
| `accepted_values` | `discount_type` | Hanya: none, loyalty, special, promo, manual, surcharge, unknown |
| `accepted_values` | `item_type` | Hanya: Shoe, Bag, Carpet, Sandal, Other |
| Custom test | `discount_gap` | Alert jika `is_discount_missed = TRUE` lebih dari 3 rows per tahun |
| `not_null` | `payment_status` | Setelah fill, tidak boleh ada null di orders dengan `total_harga > 0` |

***

## 9. Anomali yang Jadi Finding

Ini yang bisa kamu present di portfolio sebagai **business insights**:

1. **Missed loyalty discount**: Beberapa customer harusnya dapat diskon di order genap tapi tidak tercatat — karena tracking manual. Kolom `is_discount_missed` akan membuktikan ini.

2. **Free order (row 84, Angelica 2023)**: Discount = 100% (Rp50.000 dari Rp50.000). Perlu klarifikasi apakah goodwill atau error input.

3. **Surcharge (row 44, Ibel 2023)**: Discount negatif (`-20000`) artinya bayar lebih dari list price — mungkin ada biaya tambahan yang tidak ada kolomnya.

4. **Promo Agustus 2023**: Discount Rp8.000 muncul di banyak order Agustus 2023, lalu tidak ada lagi. Ini seperti ada promo sementara tapi tidak terdokumentasi.

5. **Gap 2024**: Tidak ada data order 2024 sama sekali. Perlu dicatat di dokumentasi — mungkin bisnis pause, atau data belum diinput.

6. **Revenue anomali**: `Jenis cuci = Tas` bisa punya harga lebih tinggi (Rp80.000–90.000) dari Deep Clean (Rp50.000) — ini perlu di-flag karena pricing-nya berbeda.

***

## 10. Checklist Progress

### Section 1: Setup
- [ ] Buat repo Git
- [ ] Buat folder structure: `raw/`, `dbt/models/staging/`, `dbt/models/marts/`
- [ ] Upload 2 file Excel ke `raw/`
- [ ] Setup Python ingest script (pandas → CSV / DuckDB)

### Section 2: Staging (dbt atau SQL)
- [ ] `stg_orders_2023.sql` — ingest + basic cleaning 2023
- [ ] `stg_orders_2025.sql` — ingest + basic cleaning 2025-2026
- [ ] `stg_orders.sql` — UNION kedua staging + derive columns section 3a
- [ ] `stg_expenses.sql` — ingest kedua sheet Pengeluaran

### Section 3: Marts
- [ ] `fct_orders.sql` — fact table utama dengan semua derived columns section 3b
- [ ] `dim_customers.sql` — 1 row per customer, total orders, first/last order date
- [ ] `dim_service_types.sql` — referensi jenis layanan + pricing
- [ ] `fct_expenses.sql` — fact table pengeluaran per kategori (COGS vs gaji)

### Section 4: Analysis
- [ ] Revenue per bulan / tahun
- [ ] Top customers by revenue
- [ ] Repeat rate per tahun
- [ ] Discount analysis — missed loyalty discount
- [ ] COGS vs revenue → net margin
- [ ] Kang cuci performance (2023 only, karena 2025 hanya Vian)

### Section 5: Dashboard
- [ ] Tentukan tool (Metabase / Streamlit / Looker Studio)
- [ ] Build charts

***

*Generated: April 2026 | Project: Sepokat Portfolio*