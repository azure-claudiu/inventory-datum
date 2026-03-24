"""
SnowflakeGenerator — reads snowflake_table_columns.csv, creates snowflake.db
(SQLite) with the inferred schema, and seeds it with plausible data.

Schema is derived dynamically from the CSV so the file is the single source
of truth — no separate DTOs needed.

Usage
-----
    gen = SnowflakeGenerator()   # creates snowflake.db, builds schema from CSV
    gen.seed()                   # inserts N=10 rows per table
    gen.close()

Or as a one-shot:
    python snowflake_gen.py
"""

from __future__ import annotations

import csv
import random
import re
import sqlite3
from collections import OrderedDict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants / lookup pools
# ---------------------------------------------------------------------------

_COUNTRIES      = ["DE", "US", "GB", "FR", "NL", "CH", "AT", "PL", "MX", "CA"]
_CURRENCIES     = ["EUR", "USD", "GBP", "CHF", "PLN", "CAD", "MXN"]
_LANGUAGES      = ["DE", "EN", "FR", "PL", "ES"]
_UOM            = ["EA", "KG", "L", "M", "PCE", "BOX", "FT", "IN", "LB"]
_CITIES         = ["Berlin", "New York", "London", "Paris", "Zurich",
                   "Munich", "Dallas", "Bristol", "Frankfurt", "Chicago"]
_MAT_GROUPS     = ["ELEC", "MECH", "CHEM", "FOOD", "PACK", "TEXT", "AERO"]
_MRP_TYPES      = ["PD", "VB", "VM", "MK", "ND", "NB"]
_ABC_CLASSES    = ["A", "B", "C"]
_XYZ_CLASSES    = ["X", "Y", "Z"]
_SEGMENTS       = ["OE", "SPARES", "MRO", "NPI"]
_LIFE_CYCLES    = ["ACTIVE", "MATURE", "LTB", "OBSOLETE"]
_PROC_TYPES     = ["E", "F", "X"]
_SNAP_TYPES     = ["WEEKLY", "MONTHLY", "DAILY"]
_FLAGS          = ["X", ""]
_SRC_IDS        = ["SRC01", "SRC02", "SRC03"]
_PO_TYPES       = ["NB", "UB", "0001", "0002"]
_TCODES         = ["ME21N", "ME22N", "ME23N", "MB51", "MMBE", "MIGO", "MM60"]
_REGIONS        = [f"R{i:02d}" for i in range(1, 10)]
_EXCPT_CODES    = ["10", "20", "30", "40", "50"]
_SUPPLIER_TYPES = ["DOMESTIC", "FOREIGN", "INTERCO", "SUBCON"]
_PLANT_CATS     = ["PROD", "DIST", "WHS", "SERV"]
_VALUATION_TYPES= ["", "DOMESTIC", "IMPORT", "OEM"]
_MRP_GROUPS     = ["0001", "0010", "0020", "Z001"]
_COVERAGE_PROF  = ["CP01", "CP02", "CP10", "CP_STD"]
_DCM_CLASSES    = ["GREEN", "YELLOW", "RED", "GREY"]
_STATUS_CODES   = ["A", "B", "C", ""]
_LPEIN          = ["T", "W", "M"]          # delivery date category (day/week/month)
_EBTYP          = ["AB", "LA", "LB", "LC"] # confirmation category in EKES

# Row-index-based key pools  (consistent FK-like values across tables)
_PLANT_IDS  = [f"P{i:03d}" for i in range(1, 11)]
_COMPANY_IDS= [f"{1000+i:04d}" for i in range(10)]
_VENDOR_IDS = [f"V{i+1:010d}" for i in range(10)]
_CUST_IDS   = [f"C{i+1:010d}" for i in range(10)]
_MATNR_POOL = [f"MAT{i+1:010d}" for i in range(10)]
_PO_POOL    = [f"PO{i+1:08d}" for i in range(10)]
_SNAP_IDS   = [f"SNAP{i+1:06d}" for i in range(10)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sf_to_sqlite(sf_type: str) -> str:
    """Map a Snowflake column type to the appropriate SQLite affinity."""
    sf = sf_type.upper()
    if sf in ("NUMBER", "NUMERIC", "INTEGER", "INT", "BIGINT", "SMALLINT",
              "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL"):
        return "REAL"
    # DATE and TIMESTAMP* stored as ISO-8601 TEXT
    return "TEXT"


def _sanitise(name: str) -> str:
    """Convert a Snowflake object name to a valid unquoted SQLite table name."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_").lower()


def _rand_date(start_year: int = 2020, end_year: int = 2025) -> str:
    start = date(start_year, 1, 1)
    delta = (date(end_year, 12, 31) - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def _rand_ts() -> str:
    return f"{_rand_date()} 00:00:00.000"


def _rand_amount(lo: float = 10.0, hi: float = 100_000.0) -> float:
    return round(random.uniform(lo, hi), 2)


def _rand_qty(lo: float = 0.0, hi: float = 1000.0) -> float:
    return round(random.uniform(lo, hi), 3)


# ---------------------------------------------------------------------------
# Per-column value generator
# ---------------------------------------------------------------------------

def _col_value(col: str, sf_type: str, row_idx: int) -> Any:
    """Return a plausible value for *col* (Snowflake type *sf_type*)."""
    c   = col.upper()
    cl  = col.lower()
    sft = sf_type.upper()

    # ── identity / primary key columns ──────────────────────────────────────
    if c == "MANDT":
        return "100"
    if c in ("WERKS", "PLANT_ID", "PLWRK", "WRK01", "WRK02",
             "SHIPPING_PLANT", "PLANNING_PLANT", "SUPPLYING_PLANT"):
        return _PLANT_IDS[row_idx % len(_PLANT_IDS)]
    if c in ("MATNR", "MATERIAL_NUMBER", "MATERIAL_ID", "MATL_ID", "EMATN"):
        return _MATNR_POOL[row_idx % len(_MATNR_POOL)]
    if c in ("LIFNR", "VENDOR_ID", "EMLIF"):
        return _VENDOR_IDS[row_idx % len(_VENDOR_IDS)]
    if c in ("KUNNR", "CUSTOMER_ID"):
        return _CUST_IDS[row_idx % len(_CUST_IDS)]
    if c in ("BUKRS", "COMPANY_ID", "COMPANY_CODE", "BWKEY"):
        return _COMPANY_IDS[row_idx % len(_COMPANY_IDS)]
    if c in ("EBELN", "PURCHASE_ORDER_ID", "ZORDNUM", "DTNUM"):
        return _PO_POOL[row_idx % len(_PO_POOL)]
    if c in ("EBELP", "PURCHASE_ORDER_ITEM_ID", "ZPOSNR", "DTPOS", "BNFPO"):
        return f"{(row_idx % 10 + 1) * 10:05d}"
    if c in ("ETENR", "PURCHASE_ORDER_SCHED_LINE_ID"):
        return f"{row_idx % 9 + 1:04d}"
    if c == "ETENS":
        return f"{row_idx % 9 + 1:04d}"
    if c == "SNAPSHOT_ID":
        return _SNAP_IDS[row_idx % len(_SNAP_IDS)]
    if c == "BELNR":
        return f"FI{row_idx+1:08d}"
    if c == "GJAHR":
        return str(random.choice([2022, 2023, 2024]))
    if c in ("SAKNR", "HKONT"):
        return f"{100000 + row_idx * 1000:06d}"
    if c == "KTOPL":
        return "INT"
    if c in ("VBELN",):
        return f"SO{row_idx+1:08d}"
    if c == "BANFN":
        return f"PR{row_idx+1:08d}"
    if c == "QUNUM":
        return f"QU{row_idx+1:08d}"
    if c == "BERID":
        return f"BR{row_idx % 5 + 1:03d}"
    if c in ("DISPO", "MRP_CONTROLLER_ID"):
        return f"MC{row_idx % 10 + 1:03d}"
    if c in ("PRCTR", "PRFT_CNTR_ID", "PROFIT_CENTER"):
        return f"PC{row_idx % 10 + 1:04d}"
    if c in ("KOSTL",):
        return f"CC{row_idx % 5 + 1:04d}"
    if c in ("ADDRESS_ID",):
        return f"ADDR{row_idx+1:06d}"
    if c in ("SALES_DISTRICT_ID",):
        return f"SD{row_idx % 5 + 1:03d}"
    if c in ("VALUATION_AREA_ID",):
        return _COMPANY_IDS[row_idx % len(_COMPANY_IDS)]

    # ── descriptions / names ─────────────────────────────────────────────────
    if c in ("PLANT_NAME", "PLANT_NAME_2", "NAME1"):
        return f"Plant {_PLANT_IDS[row_idx % len(_PLANT_IDS)]}"
    if c in ("PLANT_NAME_2",):
        return f"Alt Plant {_PLANT_IDS[row_idx % len(_PLANT_IDS)]}"
    if c in ("MAKTX", "MATERIAL_DESCRIPTION"):
        return f"Material {_MATNR_POOL[row_idx % len(_MATNR_POOL)]}"
    if c == "VEND_NM":
        return f"Vendor {_VENDOR_IDS[row_idx % len(_VENDOR_IDS)]} GmbH"
    if c in ("PURCHASING_GROUP_NAME",):
        return f"Purch Grp {row_idx % 5 + 1:03d}"
    if c in ("PURCHASE_ORDER_TYPE_DESCRIPTION",):
        return random.choice(["Standard PO", "STO", "Subcontracting", "Consignment"])
    if c in ("MATERIAL_GROUP_DESCRIPTION",):
        return random.choice(_MAT_GROUPS) + " Group"
    if c in ("DSNAM",):
        return f"MRP Controller {row_idx % 10 + 1}"
    if c in ("EKNAM",):
        return f"Buyer {row_idx % 5 + 1}"
    if c in ("ADDRNAME1", "ZSUBCON_NAME"):
        return f"Company {row_idx+1} Ltd"
    if c in ("MESSAGE", "AUSLT"):
        return f"Exception message {row_idx+1}"
    if c == "WBS":
        return f"WBS-{row_idx+1:05d}"

    # ── org / config codes ───────────────────────────────────────────────────
    if c in ("LAND1", "COUNTRY"):
        return random.choice(_COUNTRIES)
    if c in ("WAERS", "CURRENCY", "DOCUMENT_CURRENCY", "ZCURRENCY"):
        return random.choice(_CURRENCIES)
    if c in ("MEINS", "BASE_UOM_CD", "VRKME", "ZUOM"):
        return random.choice(_UOM)
    if c == "SPRAS":
        return random.choice(_LANGUAGES)
    if c in ("MATKL", "MATERIAL_GROUP"):
        return random.choice(_MAT_GROUPS)
    if c in ("MATERIAL_GROUP_4_ID", "MATERIAL_GROUP_5_ID"):
        return random.choice(_MAT_GROUPS)
    if c in ("MTART", "MATERIAL_TYPE"):
        return random.choice(["ROH", "FERT", "HALB", "HAWA", "DIEN"])
    if c in ("DISMM", "MRP_TYPE", "CURRENT_PLANNING_METHOD"):
        return random.choice(_MRP_TYPES)
    if c in ("BESKZ", "PROCUREMENT_TYPE"):
        return random.choice(_PROC_TYPES)
    if c in ("ABC", "CURRENT_ABC", "MAABC", "FL_CURRENT_ABC", "NO_ABC"):
        return random.choice(_ABC_CLASSES)
    if c in ("XYZ", "CURRENT_XYZ", "FL_CURRENT_XYZ"):
        return random.choice(_XYZ_CLASSES)
    if c in ("LMN", "EFG", "UVW", "PQR"):
        return random.choice(["L", "M", "N", "E", "F", "G"])
    if c in ("SEGMENT", "OE_SPARES_MRO"):
        return random.choice(_SEGMENTS)
    if c in ("LIFE_CYCLE",):
        return random.choice(_LIFE_CYCLES)
    if c in ("LTB",):
        return random.choice(["Y", "N", ""])
    if c in ("SNAPSHOT_TYPE",):
        return random.choice(_SNAP_TYPES)
    if c in ("CITY", "ORT01"):
        return random.choice(_CITIES)
    if c in ("REGIO", "REGION"):
        return random.choice(_REGIONS)
    if c in ("STRAS", "ADDRESS"):
        return f"{random.randint(1, 999)} Industrial Ave"
    if c in ("PSTLZ",):
        return f"{random.randint(10000, 99999)}"
    if c in ("EKGRP", "PURCHASE_GROUP_ID"):
        return random.choice(["001", "002", "010"])
    if c in ("EKORG", "PURCHASING_ORGANISATION_ID", "SALES_ORGANISATION_ID"):
        return random.choice(["1000", "2000"])
    if c in ("DISTRIBUTION_CHANNEL_ID",):
        return random.choice(["10", "20", "30"])
    if c in ("DIVISION_ID",):
        return random.choice(["00", "01", "10"])
    if c in ("BSART", "PURCHASE_ORDER_TYPE_ID"):
        return random.choice(_PO_TYPES)
    if c in ("EBTYP",):
        return random.choice(_EBTYP)
    if c in ("TCODE",):
        return random.choice(_TCODES)
    if c in ("TAS_SOURCE_ID", "TAS_SRC_ID", "SRC"):
        return random.choice(_SRC_IDS)
    if c in ("SUPPLIER_TYPE",):
        return random.choice(_SUPPLIER_TYPES)
    if c in ("PLANT_CATEGORY",):
        return random.choice(_PLANT_CATS)
    if c in ("VALUATION_TYPE",):
        return random.choice(_VALUATION_TYPES)
    if c in ("MRP_GROUP", "DISGR"):
        return random.choice(_MRP_GROUPS)
    if c in ("COVERAGE_PROFILE",):
        return random.choice(_COVERAGE_PROF)
    if c in ("DCM_CLASS",):
        return random.choice(_DCM_CLASSES)
    if c in ("STATU_EKKO",):
        return random.choice(_STATUS_CODES)
    if c in ("LPEIN",):
        return random.choice(_LPEIN)
    if c in ("EXCPTN_MSG_NUM", "AUSKT", "AUSSL", "DISMM"):
        return random.choice(_EXCPT_CODES)
    if c in ("CYCLE",):
        return random.choice(["A", "B", "C", "D"])
    if c in ("ZBSART",):
        return random.choice(_PO_TYPES)
    if c in ("DISMM", "DISMM"):
        return random.choice(_MRP_TYPES)
    if c in ("FACTORY_CALENDAR_CD",):
        return random.choice(["01", "US", "DE", "GB"])
    if c in ("SALES_OPER_PLANNING_PLANT_IND",):
        return random.choice(["X", ""])
    if c in ("KOART",):
        return random.choice(["S", "D", "K"])       # G/L / customer / vendor
    if c in ("BSCHL",):
        return random.choice(["40", "50", "31", "21"])
    if c in ("BLART",):
        return random.choice(["KR", "DR", "SA", "ZP"])
    if c in ("CURRENT_EMP_POLICY",):
        return random.choice(["STANDARD", "SAFETY", "EXCESS"])

    # ── flag / indicator columns ──────────────────────────────────────────────
    _FLAG_SUFFIXES = ("_IND", "KZ", "LOEKZ", "ELIKZ", "EREKZ", "EGLKZ",
                      "LVORM", "XBILK", "FIXKZ", "NODISP", "IMWRK", "KZDIS")
    if any(c.endswith(s) for s in _FLAG_SUFFIXES):
        return random.choice(_FLAGS)
    if c in ("DELET", "NEU", "DEL", "NURNULL", "NEGVERB"):
        return random.choice(_FLAGS)
    if c in ("EXCEPTION_INDICATOR8",):
        return random.choice(["", "X", "1"])

    # ── CDC / replication metadata ────────────────────────────────────────────
    if c == "REPL_CDC_OPCODE":
        return random.choice(["I", "U", "D"])
    if c == "REPL_CDC_DATE":
        return _rand_ts()

    # ── date columns ─────────────────────────────────────────────────────────
    _DATE_KEYWORDS = ("_date", "_dt", "dat", "erdat", "bedat", "eindt",
                      "bldat", "budat", "wadat", "slfdt", "mbdat", "lddat",
                      "tddat", "eldat", "altdt", "handoverdate", "dsdat",
                      "umdat", "zdock_date", "ersda", "laeda", "aedat",
                      "udate", "ldate", "fdate", "ekdat", "ekdtb",
                      "mfrgr", "iprkz")
    if sft == "DATE" or any(kw in cl for kw in _DATE_KEYWORDS):
        return _rand_date()
    if sft.startswith("TIMESTAMP") or "ts" in cl or ("change" in cl and "date" not in cl):
        return _rand_ts()

    # ── numeric columns ───────────────────────────────────────────────────────
    if sft in ("NUMBER", "NUMERIC", "FLOAT", "REAL", "DOUBLE"):
        if any(kw in cl for kw in ("value", "cost", "price", "amount",
                                   "usd", "_lc", "wert", "extcst", "unitcost")):
            return _rand_amount()
        if any(kw in cl for kw in ("qty", "quantity", "menge", "bestand",
                                   "labst", "insme", "speme", "einme",
                                   "wemng", "ameng", "glmng", "dabmg",
                                   "ormng", "demand", "consumption",
                                   "buffer", "excess", "stock", "schnit",
                                   "schnitg", "reichtg", "umsch", "boden",
                                   "bodwt", "sbrwv", "maxsb", "rf_wbz",
                                   "lbg_bedarf", "opi", "safety")):
            return _rand_qty()
        if any(kw in cl for kw in ("rate", "kurs", "factor", "abcwert",
                                   "xyzmnge")):
            return round(random.uniform(0.5, 2.0), 5)
        if any(kw in cl for kw in ("days", "day", "dys", "plifz", "wbzkt",
                                   "wbzat", "wbzekt", "zdiffdays", "zduration")):
            return float(random.randint(0, 365))
        if any(kw in cl for kw in ("count", "cnt", "anzsn", "mahnz",
                                   "trade_off_zone")):
            return float(random.randint(0, 100))
        if any(kw in cl for kw in ("prio", "priority", "matprio")):
            return float(random.randint(1, 10))
        if any(kw in cl for kw in ("created_on",)):
            return float(int(_rand_date().replace("-", "")))
        return _rand_amount(0, 10000)

    # ── TEXT fallback ─────────────────────────────────────────────────────────
    return f"{col[:8]}_{row_idx+1:03d}"


# ---------------------------------------------------------------------------
# Schema parser
# ---------------------------------------------------------------------------

def _parse_schema(csv_path: Path) -> "OrderedDict[str, list[tuple[str,str]]]":
    """
    Read the CSV and return an ordered mapping of
    ``original_table_name → [(col_name, sf_type), …]`` in column order.
    """
    schema: OrderedDict[str, list[tuple[str, str]]] = OrderedDict()
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            tbl  = row["object_name"].strip()
            col  = row["column_name"].strip()
            typ  = row["data_type"].strip()
            err  = row.get("error", "").strip()
            if err:          # skip rows where column info could not be retrieved
                continue
            if tbl not in schema:
                schema[tbl] = []
            schema[tbl].append((col, typ))
    return schema


# ---------------------------------------------------------------------------
# SnowflakeGenerator
# ---------------------------------------------------------------------------

class SnowflakeGenerator:
    """
    Reads *snowflake_table_columns.csv* to infer schema, creates *snowflake.db*,
    and seeds every table with *n* rows of plausible data.

    Parameters
    ----------
    db_path    : path to the target SQLite database file
    csv_path   : path to snowflake_table_columns.csv
    n          : rows to insert per table
    seed       : random seed for reproducibility
    """

    def __init__(
        self,
        db_path: str  = "snowflake.db",
        csv_path: str = "snowflake_table_columns.csv",
        n: int        = 10,
        seed: int     = 42,
    ) -> None:
        self.db_path  = db_path
        self.n        = n
        random.seed(seed)

        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"Schema CSV not found: {csv_file.resolve()}")

        self._schema = _parse_schema(csv_file)

        # Build mapping: original name → SQLite-safe name
        # Collision handling: append _2, _3 … if two names sanitise identically.
        seen: dict[str, int] = {}
        self._table_map: dict[str, str] = {}
        for orig in self._schema:
            base = _sanitise(orig)
            if base in seen:
                seen[base] += 1
                safe = f"{base}_{seen[base]}"
            else:
                seen[base] = 1
                safe = base
            self._table_map[orig] = safe

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_schema(self) -> None:
        cur = self.conn.cursor()
        for orig, columns in self._schema.items():
            safe = self._table_map[orig]
            col_defs = ",\n            ".join(
                f'"{col}" {_sf_to_sqlite(typ)}' for col, typ in columns
            )
            ddl = f"""
            CREATE TABLE IF NOT EXISTS "{safe}" (
                {col_defs}
            )
            """
            cur.execute(ddl)
        self.conn.commit()

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def seed(self) -> None:
        """Insert *n* rows into every table. Safe to call multiple times."""
        cur  = self.conn.cursor()
        n    = self.n
        totals: dict[str, int] = {}

        for orig, columns in self._schema.items():
            safe = self._table_map[orig]
            placeholders = ", ".join(["?"] * len(columns))
            stmt = f'INSERT OR IGNORE INTO "{safe}" VALUES ({placeholders})'

            rows = [
                tuple(_col_value(col, typ, i) for col, typ in columns)
                for i in range(n)
            ]
            cur.executemany(stmt, rows)
            totals[safe] = n

        self.conn.commit()

        total_rows = sum(totals.values())
        print(
            f"Seeded {len(totals)} tables × {n} rows = {total_rows} total rows "
            f"-> {self.db_path}"
        )
        if len(self._table_map) <= 30:
            for orig, safe in self._table_map.items():
                print(f"  {orig!s:40s} -> {safe}")

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------
    # Convenience: return table name map
    # ------------------------------------------------------------------

    @property
    def table_map(self) -> dict[str, str]:
        """Mapping of original Snowflake object name → SQLite table name."""
        return dict(self._table_map)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db       = Path("snowflake.db")
    csv_file = Path("snowflake_table_columns.csv")

    gen = SnowflakeGenerator(db_path=str(db), csv_path=str(csv_file), n=10)
    gen.seed()
    gen.close()
    print(f"\nDone. Database: {db.resolve()}")
