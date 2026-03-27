# inventory-data

Synthetic data generators for two source systems used in supply-chain / inventory analytics work: **SAP** and **Snowflake**. Each generator creates a local SQLite database seeded with plausible fake data that mirrors the real schema, so downstream tooling (queries, dashboards, AI agents) can be developed and tested without touching production systems.

---

## How to run the scripts

```bash
python sap_gen.py
python snowflake_gen.py
```

## Files

| File | Description |
|------|-------------|
| `sap_dtos.py` | Python dataclasses modelling the SAP tables (Basis, MM, SD, FI modules) |
| `sap_gen.py` | Creates `sap.db` and seeds it with SAP-style data |
| `snowflake_table_columns.csv` | Schema export from Snowflake — source of truth for all table/column definitions |
| `snowflake_gen.py` | Reads the CSV, creates `snowflake.db`, and seeds it |

NOTE: The `snowflake_table_columns.csv` is not included in the repo due to its proprietary nature. Please ask the team for access.

---

## sap_gen.py

Generates a SQLite database (`sap.db`) with 20 tables covering four SAP modules.

### Schema

| Module | Tables |
|--------|--------|
| Basis | `t001` (Company Code), `t001w` (Plant), `t001l` (Storage Location) |
| MM — Material Master | `mara`, `marc`, `mard` |
| MM — Vendor / PO | `lfa1`, `lfb1`, `ekko`, `ekpo` |
| SD — Customer / Sales | `kna1`, `knb1`, `vbak`, `vbap`, `likp`, `lips` |
| FI — G/L & Documents | `ska1`, `skb1`, `bkpf`, `bseg` |

Tables are created with proper primary and foreign keys, inserted in dependency order, and the FK constraint `PRAGMA foreign_keys = ON` is enabled.

### Usage

```python
from sap_gen import SapGenerator

gen = SapGenerator()     # creates sap.db with schema
gen.seed()               # inserts 10 rows per table (20 rows in bseg)
gen.close()
```

```bash
python sap_gen.py        # one-shot: creates sap.db with N=10
```

**Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_path` | `"sap.db"` | Output SQLite file |
| `n` | `10` | Rows to insert per table |
| `seed` | `42` | Random seed for reproducibility |

---

## snowflake_gen.py

Generates a SQLite database (`snowflake.db`) from the full Snowflake schema defined in `snowflake_table_columns.csv`. The CSV is the single source of truth — no schema is hardcoded in the script.

### Schema

28 tables across two Snowflake schemas:

**COL_PUBLISHED** — transformed / analytical views

| Table | Columns | Description |
|-------|---------|-------------|
| `AIML_OPEN_PURCHASE_ORDERS` | 47 | Open PO lines with exception and quantity data |
| `CORE_PLANT` | 23 | Plant master with org assignments |
| `EDW_INVENTORY_SEGMENTATION_SNAPSHOT` | 113 | Inventory segmentation snapshot (ABC/XYZ, stock by type, coverage) |
| `EDW_MATL_LOC_DEMAND_INFO` | 24 | Rolling demand statistics by material/plant |

**COL_RAW_EXPORT** — raw SAP table extracts

| Table | Columns | Description |
|-------|---------|-------------|
| `EKES` | 34 | PO order acknowledgements |
| `EKET` | 97 | PO schedule lines |
| `EKKO` | 165 | PO headers |
| `EKPO` | 352 | PO items |
| `LFA1` | 483 | Vendor master |
| `LIPS` | ~50 | Delivery items |
| `MAKT` | 6 | Material descriptions |
| `MARA` | 279 | Material master — general data |
| `MARC` | 275 | Material master — plant data |
| `MARD` | 52 | Material master — storage location stock |
| `MBEW` | 110 | Material valuation |
| `MSKU` | 33 | Special stocks at customer |
| `MSLB` | 28 | Special stocks at vendor |
| `MSPR` | 32 | Project stock |
| `MVKE` | 75 | Material master — sales data |
| `T001` | 80 | Company codes |
| `T001K` | 24 | Valuation areas |
| `T001W` | 66 | Plants |
| `T023T` | 6 | Material group descriptions |
| `T024` | 10 | Purchasing groups |
| `T438R` | 19 | MRP controller assignments |
| `TCURR` | 9 | Exchange rates |
| `/SAPLOM/MEH_MM01` | 325 | Custom SCR planning & buffer management data |
| `ZSCR043_01_EXCP` | 76 | Custom SCR exception report |

Snowflake object names are sanitised for SQLite (e.g. `/SAPLOM/MEH_MM01` → `saplom_meh_mm01`). Column names with special characters are quoted.

### Usage

```python
from snowflake_gen import SnowflakeGenerator

gen = SnowflakeGenerator()   # reads snowflake_table_columns.csv, creates snowflake.db
gen.seed()                   # inserts 10 rows per table
gen.close()

print(gen.table_map)         # dict of original Snowflake name -> SQLite table name
```

```bash
python snowflake_gen.py      # one-shot: creates snowflake.db with N=10
```

**Parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_path` | `"snowflake.db"` | Output SQLite file |
| `csv_path` | `"snowflake_table_columns.csv"` | Schema definition CSV |
| `n` | `10` | Rows to insert per table |
| `seed` | `42` | Random seed for reproducibility |

---

## Requirements

```
Python 3.9+
```

No third-party packages are needed to run the generators — only the standard library (`sqlite3`, `csv`, `random`, `re`, `pathlib`).

`requirements.txt` lists optional packages for further development: `pydantic` for DTO validation, `pytest`/`mypy` for testing and type checking.

```bash
pip install -r requirements.txt
```
