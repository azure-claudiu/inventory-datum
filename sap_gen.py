"""
SapGenerator — creates / connects to sap.db (SQLite) and seeds it with
plausible SAP data.

Usage
-----
    gen = SapGenerator()   # creates sap.db, builds schema
    gen.seed()             # inserts N=10 rows per table
    gen.close()

Or as a one-shot:
    python sap_gen.py
"""

from __future__ import annotations

import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from sap_dtos import (
    DeliveryType,
    FIDocumentType,
    MaterialType,
    PostingKey,
    PurchaseOrderType,
    SalesDocumentType,
)

# ---------------------------------------------------------------------------
# Lookup pools for realistic SAP values
# ---------------------------------------------------------------------------

_COUNTRIES      = ["DE", "US", "GB", "FR", "NL", "CH", "AT", "PL"]
_CURRENCIES     = ["EUR", "USD", "GBP", "CHF", "PLN"]
_LANGUAGES      = ["DE", "EN", "FR", "PL"]
_UOM            = ["EA", "KG", "L", "M", "PCE", "BOX"]
_WEIGHT_UNITS   = ["KG", "G", "LB"]
_VOLUME_UNITS   = ["L", "ML", "GAL"]
_ACCT_GRP_VND   = ["0001", "0002", "KRED"]
_ACCT_GRP_CST   = ["0001", "DEBI", "KUNA"]
_SALES_ORGS     = ["1000", "2000", "3000"]
_DIST_CHANNELS  = ["10", "20", "30"]
_DIVISIONS      = ["00", "01", "10"]
_PURCH_ORGS     = ["1000", "2000"]
_PURCH_GROUPS   = ["001", "002", "010"]
_PAYMENT_TERMS  = ["NT30", "NT60", "2/10NET30"]
_PAY_METHODS    = ["C", "T", "CT"]
_TCODES         = ["FB01", "FB50", "MIRO", "VF01", "ME21N"]
_COST_CENTRES   = [f"CC{i:04d}" for i in range(1, 6)]
_MAT_GROUPS     = ["ELEC", "MECH", "CHEM", "FOOD", "PACK", "TEXT"]
_CITIES         = ["Berlin", "New York", "London", "Paris", "Zurich",
                   "Munich", "Dallas", "Bristol", "Frankfurt", "Chicago"]
_GL_DESCRIPTIONS = [
    "Cash", "Accounts Receivable", "Inventory", "Fixed Assets",
    "Accounts Payable", "Revenue", "COGS", "Salaries",
    "Depreciation", "Tax Payable",
]
_CHART_OF_ACCOUNTS = "INT"


def _rand_date(start_year: int = 2020, end_year: int = 2024) -> str:
    start = date(start_year, 1, 1)
    delta = (date(end_year, 12, 31) - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def _rand_amount(lo: float = 10.0, hi: float = 100_000.0) -> float:
    return round(random.uniform(lo, hi), 2)


def _rand_qty(lo: int = 1, hi: int = 1000) -> float:
    return float(random.randint(lo, hi))


# ---------------------------------------------------------------------------
# SapGenerator
# ---------------------------------------------------------------------------

class SapGenerator:
    """Creates sap.db with SAP-schema tables and seeds them with fake data."""

    MANDT = "100"

    # ------------------------------------------------------------------
    # DDL — one CREATE TABLE IF NOT EXISTS per SAP table, FK-enforced.
    # Insertion order matches dependency order (parents before children).
    # ------------------------------------------------------------------

    _DDL: list[str] = [
        # 1. T001 — Company Code (root, no FK deps)
        """
        CREATE TABLE IF NOT EXISTS t001 (
            mandt TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            butxt TEXT NOT NULL,
            ort01 TEXT NOT NULL,
            land1 TEXT NOT NULL,
            waers TEXT NOT NULL,
            spras TEXT NOT NULL,
            PRIMARY KEY (mandt, bukrs)
        )
        """,

        # 2. T001W — Plant  (→ t001)
        """
        CREATE TABLE IF NOT EXISTS t001w (
            mandt TEXT NOT NULL,
            werks TEXT NOT NULL,
            name1 TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            land1 TEXT NOT NULL,
            regio TEXT NOT NULL,
            stras TEXT NOT NULL,
            pstlz TEXT NOT NULL,
            ort01 TEXT NOT NULL,
            PRIMARY KEY (mandt, werks),
            FOREIGN KEY (mandt, bukrs) REFERENCES t001 (mandt, bukrs)
        )
        """,

        # 3. T001L — Storage Location  (→ t001w)
        """
        CREATE TABLE IF NOT EXISTS t001l (
            mandt TEXT NOT NULL,
            werks TEXT NOT NULL,
            lgort TEXT NOT NULL,
            lgobe TEXT NOT NULL,
            PRIMARY KEY (mandt, werks, lgort),
            FOREIGN KEY (mandt, werks) REFERENCES t001w (mandt, werks)
        )
        """,

        # 4. SKA1 — G/L Account: Chart of Accounts  (no FK deps)
        """
        CREATE TABLE IF NOT EXISTS ska1 (
            mandt TEXT NOT NULL,
            ktopl TEXT NOT NULL,
            saknr TEXT NOT NULL,
            ktoks TEXT NOT NULL,
            xbilk TEXT NOT NULL,
            gvtyp TEXT,
            txt20 TEXT NOT NULL,
            txt50 TEXT NOT NULL,
            PRIMARY KEY (mandt, ktopl, saknr)
        )
        """,

        # 5. SKB1 — G/L Account: Company Code Data  (→ t001)
        """
        CREATE TABLE IF NOT EXISTS skb1 (
            mandt TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            saknr TEXT NOT NULL,
            xspeb TEXT NOT NULL,
            xopvw TEXT NOT NULL,
            xkres TEXT NOT NULL,
            waers TEXT NOT NULL,
            PRIMARY KEY (mandt, bukrs, saknr),
            FOREIGN KEY (mandt, bukrs) REFERENCES t001 (mandt, bukrs)
        )
        """,

        # 6. LFA1 — Vendor: General Data  (no FK deps)
        """
        CREATE TABLE IF NOT EXISTS lfa1 (
            mandt TEXT NOT NULL,
            lifnr TEXT NOT NULL,
            land1 TEXT NOT NULL,
            name1 TEXT NOT NULL,
            name2 TEXT,
            ort01 TEXT NOT NULL,
            pstlz TEXT NOT NULL,
            stras TEXT NOT NULL,
            telf1 TEXT,
            ktokk TEXT NOT NULL,
            PRIMARY KEY (mandt, lifnr)
        )
        """,

        # 7. LFB1 — Vendor: Company Code Data  (→ lfa1, t001)
        """
        CREATE TABLE IF NOT EXISTS lfb1 (
            mandt TEXT NOT NULL,
            lifnr TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            akont TEXT NOT NULL,
            zterm TEXT NOT NULL,
            zwels TEXT NOT NULL,
            PRIMARY KEY (mandt, lifnr, bukrs),
            FOREIGN KEY (mandt, lifnr) REFERENCES lfa1 (mandt, lifnr),
            FOREIGN KEY (mandt, bukrs) REFERENCES t001 (mandt, bukrs)
        )
        """,

        # 8. KNA1 — Customer: General Data  (no FK deps)
        """
        CREATE TABLE IF NOT EXISTS kna1 (
            mandt TEXT NOT NULL,
            kunnr TEXT NOT NULL,
            land1 TEXT NOT NULL,
            name1 TEXT NOT NULL,
            name2 TEXT,
            ort01 TEXT NOT NULL,
            pstlz TEXT NOT NULL,
            stras TEXT NOT NULL,
            telf1 TEXT,
            ktokd TEXT NOT NULL,
            PRIMARY KEY (mandt, kunnr)
        )
        """,

        # 9. KNB1 — Customer: Company Code Data  (→ kna1, t001)
        """
        CREATE TABLE IF NOT EXISTS knb1 (
            mandt TEXT NOT NULL,
            kunnr TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            akont TEXT NOT NULL,
            zterm TEXT NOT NULL,
            PRIMARY KEY (mandt, kunnr, bukrs),
            FOREIGN KEY (mandt, kunnr) REFERENCES kna1 (mandt, kunnr),
            FOREIGN KEY (mandt, bukrs) REFERENCES t001 (mandt, bukrs)
        )
        """,

        # 10. MARA — Material Master: General Data  (no FK deps)
        """
        CREATE TABLE IF NOT EXISTS mara (
            mandt TEXT NOT NULL,
            matnr TEXT NOT NULL,
            ersda TEXT NOT NULL,
            ernam TEXT NOT NULL,
            mtart TEXT NOT NULL,
            matkl TEXT NOT NULL,
            meins TEXT NOT NULL,
            brgew REAL NOT NULL,
            ntgew REAL NOT NULL,
            gewei TEXT NOT NULL,
            volum REAL NOT NULL,
            voleh TEXT NOT NULL,
            mfrpn TEXT,
            ean11 TEXT,
            PRIMARY KEY (mandt, matnr)
        )
        """,

        # 11. MARC — Material Master: Plant Data  (→ mara, t001w)
        """
        CREATE TABLE IF NOT EXISTS marc (
            mandt TEXT NOT NULL,
            matnr TEXT NOT NULL,
            werks TEXT NOT NULL,
            pstat TEXT NOT NULL,
            lvorm TEXT NOT NULL,
            beskz TEXT NOT NULL,
            sobsl TEXT,
            minbe REAL NOT NULL DEFAULT 0,
            eisbe REAL NOT NULL DEFAULT 0,
            mtvfp TEXT NOT NULL DEFAULT '02',
            PRIMARY KEY (mandt, matnr, werks),
            FOREIGN KEY (mandt, matnr) REFERENCES mara  (mandt, matnr),
            FOREIGN KEY (mandt, werks) REFERENCES t001w (mandt, werks)
        )
        """,

        # 12. MARD — Material Master: Storage Location Data  (→ marc, t001l)
        """
        CREATE TABLE IF NOT EXISTS mard (
            mandt TEXT NOT NULL,
            matnr TEXT NOT NULL,
            werks TEXT NOT NULL,
            lgort TEXT NOT NULL,
            labst REAL NOT NULL,
            umlme REAL NOT NULL,
            insme REAL NOT NULL,
            speme REAL NOT NULL,
            einme REAL NOT NULL,
            PRIMARY KEY (mandt, matnr, werks, lgort),
            FOREIGN KEY (mandt, matnr, werks) REFERENCES marc  (mandt, matnr, werks),
            FOREIGN KEY (mandt, werks, lgort) REFERENCES t001l (mandt, werks, lgort)
        )
        """,

        # 13. EKKO — Purchase Order Header  (→ t001, lfa1)
        """
        CREATE TABLE IF NOT EXISTS ekko (
            mandt TEXT NOT NULL,
            ebeln TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            bstyp TEXT NOT NULL,
            bsart TEXT NOT NULL,
            lifnr TEXT NOT NULL,
            ekgrp TEXT NOT NULL,
            ekorg TEXT NOT NULL,
            waers TEXT NOT NULL,
            wkurs REAL NOT NULL,
            bedat TEXT NOT NULL,
            kdatb TEXT,
            kdate TEXT,
            PRIMARY KEY (mandt, ebeln),
            FOREIGN KEY (mandt, bukrs) REFERENCES t001 (mandt, bukrs),
            FOREIGN KEY (mandt, lifnr) REFERENCES lfa1 (mandt, lifnr)
        )
        """,

        # 14. EKPO — Purchase Order Item  (→ ekko, mara, t001w)
        """
        CREATE TABLE IF NOT EXISTS ekpo (
            mandt TEXT NOT NULL,
            ebeln TEXT NOT NULL,
            ebelp TEXT NOT NULL,
            matnr TEXT NOT NULL,
            werks TEXT NOT NULL,
            lgort TEXT,
            menge REAL NOT NULL,
            meins TEXT NOT NULL,
            netpr REAL NOT NULL,
            peinh REAL NOT NULL,
            eindt TEXT NOT NULL,
            elikz TEXT NOT NULL,
            erekz TEXT NOT NULL,
            PRIMARY KEY (mandt, ebeln, ebelp),
            FOREIGN KEY (mandt, ebeln) REFERENCES ekko  (mandt, ebeln),
            FOREIGN KEY (mandt, matnr) REFERENCES mara  (mandt, matnr),
            FOREIGN KEY (mandt, werks) REFERENCES t001w (mandt, werks)
        )
        """,

        # 15. VBAK — Sales Order Header  (→ kna1)
        """
        CREATE TABLE IF NOT EXISTS vbak (
            mandt TEXT NOT NULL,
            vbeln TEXT NOT NULL,
            auart TEXT NOT NULL,
            audat TEXT NOT NULL,
            vkorg TEXT NOT NULL,
            vtweg TEXT NOT NULL,
            spart TEXT NOT NULL,
            kunnr TEXT NOT NULL,
            waerk TEXT NOT NULL,
            netwr REAL NOT NULL,
            knumv TEXT NOT NULL,
            PRIMARY KEY (mandt, vbeln),
            FOREIGN KEY (mandt, kunnr) REFERENCES kna1 (mandt, kunnr)
        )
        """,

        # 16. VBAP — Sales Order Item  (→ vbak, mara, t001w)
        """
        CREATE TABLE IF NOT EXISTS vbap (
            mandt TEXT NOT NULL,
            vbeln TEXT NOT NULL,
            posnr TEXT NOT NULL,
            matnr TEXT NOT NULL,
            werks TEXT NOT NULL,
            menge REAL NOT NULL,
            meins TEXT NOT NULL,
            netwr REAL NOT NULL,
            netpr REAL NOT NULL,
            pmatn TEXT,
            abgru TEXT,
            PRIMARY KEY (mandt, vbeln, posnr),
            FOREIGN KEY (mandt, vbeln) REFERENCES vbak  (mandt, vbeln),
            FOREIGN KEY (mandt, matnr) REFERENCES mara  (mandt, matnr),
            FOREIGN KEY (mandt, werks) REFERENCES t001w (mandt, werks)
        )
        """,

        # 17. LIKP — Delivery Header  (→ t001w, kna1)
        """
        CREATE TABLE IF NOT EXISTS likp (
            mandt TEXT NOT NULL,
            vbeln TEXT NOT NULL,
            lfart TEXT NOT NULL,
            wadat TEXT NOT NULL,
            werks TEXT NOT NULL,
            kunnr TEXT NOT NULL,
            anzpk INTEGER NOT NULL,
            PRIMARY KEY (mandt, vbeln),
            FOREIGN KEY (mandt, werks) REFERENCES t001w (mandt, werks),
            FOREIGN KEY (mandt, kunnr) REFERENCES kna1  (mandt, kunnr)
        )
        """,

        # 18. LIPS — Delivery Item  (→ likp, mara, t001l)
        """
        CREATE TABLE IF NOT EXISTS lips (
            mandt TEXT NOT NULL,
            vbeln TEXT NOT NULL,
            posnr TEXT NOT NULL,
            matnr TEXT NOT NULL,
            werks TEXT NOT NULL,
            lgort TEXT NOT NULL,
            lfimg REAL NOT NULL,
            vrkme TEXT NOT NULL,
            PRIMARY KEY (mandt, vbeln, posnr),
            FOREIGN KEY (mandt, vbeln)       REFERENCES likp  (mandt, vbeln),
            FOREIGN KEY (mandt, matnr)       REFERENCES mara  (mandt, matnr),
            FOREIGN KEY (mandt, werks, lgort) REFERENCES t001l (mandt, werks, lgort)
        )
        """,

        # 19. BKPF — FI Document Header  (→ t001)
        """
        CREATE TABLE IF NOT EXISTS bkpf (
            mandt TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            belnr TEXT NOT NULL,
            gjahr TEXT NOT NULL,
            blart TEXT NOT NULL,
            bldat TEXT NOT NULL,
            budat TEXT NOT NULL,
            usnam TEXT NOT NULL,
            tcode TEXT NOT NULL,
            bktxt TEXT,
            stblg TEXT,
            stjah TEXT,
            PRIMARY KEY (mandt, bukrs, belnr, gjahr),
            FOREIGN KEY (mandt, bukrs) REFERENCES t001 (mandt, bukrs)
        )
        """,

        # 20. BSEG — FI Document Item  (→ bkpf)
        # Note: generates 2 lines per document (debit + credit) so row count = 2 * N
        """
        CREATE TABLE IF NOT EXISTS bseg (
            mandt TEXT NOT NULL,
            bukrs TEXT NOT NULL,
            belnr TEXT NOT NULL,
            gjahr TEXT NOT NULL,
            buzei TEXT NOT NULL,
            bschl TEXT NOT NULL,
            koart TEXT NOT NULL,
            hkont TEXT NOT NULL,
            wrbtr REAL NOT NULL,
            dmbtr REAL NOT NULL,
            waers TEXT NOT NULL,
            sgtxt TEXT,
            lifnr TEXT,
            kunnr TEXT,
            kostl TEXT,
            aufnr TEXT,
            PRIMARY KEY (mandt, bukrs, belnr, gjahr, buzei),
            FOREIGN KEY (mandt, bukrs, belnr, gjahr)
                REFERENCES bkpf (mandt, bukrs, belnr, gjahr)
        )
        """,
    ]

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, db_path: str = "sap.db", n: int = 10, seed: int = 42):
        self.db_path = db_path
        self.n = n
        random.seed(seed)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_schema(self) -> None:
        cur = self.conn.cursor()
        for stmt in self._DDL:
            cur.execute(stmt)
        self.conn.commit()

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def seed(self) -> None:
        """Insert N rows into every table. Safe to call multiple times (INSERT OR IGNORE)."""
        m = self.MANDT
        n = self.n

        # -- Key pools (1-to-1 by index so FKs stay consistent) -------
        bukrs_pool  = [f"{1000 + i:04d}"        for i in range(n)]
        werks_pool  = [f"W{100 + i:03d}"         for i in range(n)]
        lgort_pool  = [f"L{100 + i:03d}"         for i in range(n)]
        saknr_pool  = [f"{100000 + i * 1000:06d}" for i in range(n)]
        lifnr_pool  = [f"V{i + 1:010d}"          for i in range(n)]
        kunnr_pool  = [f"C{i + 1:010d}"          for i in range(n)]
        matnr_pool  = [f"MAT{i + 1:010d}"        for i in range(n)]
        ebeln_pool  = [f"PO{i + 1:08d}"          for i in range(n)]
        so_vbeln    = [f"SO{i + 1:08d}"          for i in range(n)]
        del_vbeln   = [f"DL{i + 1:08d}"          for i in range(n)]
        belnr_pool  = [f"FI{i + 1:08d}"          for i in range(n)]
        gjahr       = "2024"

        cur = self.conn.cursor()

        def ins(table: str, rows: list[tuple]) -> None:
            if not rows:
                return
            placeholders = ", ".join(["?"] * len(rows[0]))
            cur.executemany(
                f"INSERT OR IGNORE INTO {table} VALUES ({placeholders})", rows
            )

        # 1. T001 — Company Code
        ins("t001", [
            (
                m,
                bukrs_pool[i],
                f"Company {bukrs_pool[i]}",
                random.choice(_CITIES),
                random.choice(_COUNTRIES),
                random.choice(_CURRENCIES),
                random.choice(_LANGUAGES),
            )
            for i in range(n)
        ])

        # 2. T001W — Plant  (plant i → company i)
        ins("t001w", [
            (
                m,
                werks_pool[i],
                f"Plant {werks_pool[i]}",
                bukrs_pool[i],                          # FK → t001
                random.choice(_COUNTRIES),
                f"R{random.randint(1, 9):02d}",
                f"{random.randint(1, 999)} Industrial Ave",
                f"{random.randint(10000, 99999)}",
                random.choice(_CITIES),
            )
            for i in range(n)
        ])

        # 3. T001L — Storage Location  (sloc i → plant i)
        ins("t001l", [
            (m, werks_pool[i], lgort_pool[i], f"Storage Loc {lgort_pool[i]}")
            for i in range(n)
        ])

        # 4. SKA1 — G/L Account Master
        ins("ska1", [
            (
                m,
                _CHART_OF_ACCOUNTS,
                saknr_pool[i],
                random.choice(["LIAB", "ASSET", "REVN", "EXPN"]),
                random.choice(["X", ""]),
                random.choice(["X", None]),
                _GL_DESCRIPTIONS[i % len(_GL_DESCRIPTIONS)][:20],
                _GL_DESCRIPTIONS[i % len(_GL_DESCRIPTIONS)],
            )
            for i in range(n)
        ])

        # 5. SKB1 — G/L Account: Company Code Data  (account i → company i)
        ins("skb1", [
            (m, bukrs_pool[i], saknr_pool[i], "", "X", "X", random.choice(_CURRENCIES))
            for i in range(n)
        ])

        # 6. LFA1 — Vendor: General Data
        ins("lfa1", [
            (
                m,
                lifnr_pool[i],
                random.choice(_COUNTRIES),
                f"Vendor {lifnr_pool[i]} GmbH",
                None,
                random.choice(_CITIES),
                f"{random.randint(10000, 99999)}",
                f"{random.randint(1, 999)} Supplier St",
                f"+{random.randint(10, 99)}{random.randint(100000000, 999999999)}",
                random.choice(_ACCT_GRP_VND),
            )
            for i in range(n)
        ])

        # 7. LFB1 — Vendor: Company Code Data  (vendor i → company i)
        ins("lfb1", [
            (
                m,
                lifnr_pool[i],
                bukrs_pool[i],                          # FK → t001
                saknr_pool[i % len(saknr_pool)],        # reconciliation account
                random.choice(_PAYMENT_TERMS),
                random.choice(_PAY_METHODS),
            )
            for i in range(n)
        ])

        # 8. KNA1 — Customer: General Data
        ins("kna1", [
            (
                m,
                kunnr_pool[i],
                random.choice(_COUNTRIES),
                f"Customer {kunnr_pool[i]} Ltd",
                None,
                random.choice(_CITIES),
                f"{random.randint(10000, 99999)}",
                f"{random.randint(1, 999)} Client Blvd",
                f"+{random.randint(10, 99)}{random.randint(100000000, 999999999)}",
                random.choice(_ACCT_GRP_CST),
            )
            for i in range(n)
        ])

        # 9. KNB1 — Customer: Company Code Data  (customer i → company i)
        ins("knb1", [
            (
                m,
                kunnr_pool[i],
                bukrs_pool[i],                          # FK → t001
                saknr_pool[i % len(saknr_pool)],        # reconciliation account
                random.choice(_PAYMENT_TERMS),
            )
            for i in range(n)
        ])

        # 10. MARA — Material Master
        ins("mara", [
            (
                m,
                matnr_pool[i],
                _rand_date(2018, 2023),
                f"USER{random.randint(1, 50):03d}",
                random.choice([e.value for e in MaterialType]),
                random.choice(_MAT_GROUPS),
                random.choice(_UOM),
                round(random.uniform(0.1, 500.0), 3),   # brgew
                round(random.uniform(0.1, 490.0), 3),   # ntgew
                random.choice(_WEIGHT_UNITS),
                round(random.uniform(0.1, 200.0), 3),   # volum
                random.choice(_VOLUME_UNITS),
                None,                                   # mfrpn
                None,                                   # ean11
            )
            for i in range(n)
        ])

        # 11. MARC — Material: Plant Data  (material i × plant i)
        ins("marc", [
            (
                m,
                matnr_pool[i],
                werks_pool[i],                          # FK → t001w
                "V",                                    # fully maintained
                "",                                     # not deletion-flagged
                random.choice(["E", "F", "X"]),         # beskz
                None,                                   # sobsl
                round(random.uniform(0, 100), 0),       # minbe
                round(random.uniform(0, 50), 0),        # eisbe
                "02",                                   # mtvfp
            )
            for i in range(n)
        ])

        # 12. MARD — Material: Storage Location Data  (material i × plant i × sloc i)
        ins("mard", [
            (
                m,
                matnr_pool[i],
                werks_pool[i],                          # FK → marc
                lgort_pool[i],                          # FK → t001l
                _rand_qty(0, 5000),                     # labst
                _rand_qty(0, 100),                      # umlme
                _rand_qty(0, 50),                       # insme
                _rand_qty(0, 20),                       # speme
                _rand_qty(0, 5000),                     # einme
            )
            for i in range(n)
        ])

        # 13. EKKO — Purchase Order Header
        ins("ekko", [
            (
                m,
                ebeln_pool[i],
                bukrs_pool[i],                          # FK → t001
                "F",                                    # standard PO category
                random.choice([e.value for e in PurchaseOrderType]),
                lifnr_pool[i],                          # FK → lfa1
                random.choice(_PURCH_GROUPS),
                random.choice(_PURCH_ORGS),
                random.choice(_CURRENCIES),
                round(random.uniform(0.8, 1.5), 5),     # wkurs
                _rand_date(2023, 2024),
                None,                                   # kdatb
                None,                                   # kdate
            )
            for i in range(n)
        ])

        # 14. EKPO — Purchase Order Item  (one item "00010" per PO)
        ins("ekpo", [
            (
                m,
                ebeln_pool[i],                          # FK → ekko
                "00010",
                matnr_pool[i],                          # FK → mara
                werks_pool[i],                          # FK → t001w
                lgort_pool[i],                          # FK → t001l (optional, but set)
                _rand_qty(1, 500),                      # menge
                random.choice(_UOM),
                _rand_amount(5.0, 5000.0),              # netpr
                1.0,                                    # peinh
                _rand_date(2024, 2025),                 # eindt
                "",                                     # elikz
                "",                                     # erekz
            )
            for i in range(n)
        ])

        # 15. VBAK — Sales Order Header
        ins("vbak", [
            (
                m,
                so_vbeln[i],
                random.choice([e.value for e in SalesDocumentType]),
                _rand_date(2023, 2024),
                random.choice(_SALES_ORGS),
                random.choice(_DIST_CHANNELS),
                random.choice(_DIVISIONS),
                kunnr_pool[i],                          # FK → kna1
                random.choice(_CURRENCIES),
                _rand_amount(100.0, 200_000.0),         # netwr
                f"KNUMV{i:05d}",
            )
            for i in range(n)
        ])

        # 16. VBAP — Sales Order Item  (one item "000010" per order)
        ins("vbap", [
            (
                m,
                so_vbeln[i],                            # FK → vbak
                "000010",
                matnr_pool[i],                          # FK → mara
                werks_pool[i],                          # FK → t001w
                _rand_qty(1, 200),                      # menge
                random.choice(_UOM),
                _rand_amount(50.0, 50_000.0),           # netwr
                _rand_amount(5.0, 500.0),               # netpr
                None,                                   # pmatn
                None,                                   # abgru
            )
            for i in range(n)
        ])

        # 17. LIKP — Delivery Header
        ins("likp", [
            (
                m,
                del_vbeln[i],
                random.choice([e.value for e in DeliveryType]),
                _rand_date(2024, 2025),
                werks_pool[i],                          # FK → t001w
                kunnr_pool[i],                          # FK → kna1
                random.randint(1, 10),                  # anzpk
            )
            for i in range(n)
        ])

        # 18. LIPS — Delivery Item  (one item "000010" per delivery)
        ins("lips", [
            (
                m,
                del_vbeln[i],                           # FK → likp
                "000010",
                matnr_pool[i],                          # FK → mara
                werks_pool[i],                          # FK → t001l (werks part)
                lgort_pool[i],                          # FK → t001l (lgort part)
                _rand_qty(1, 200),                      # lfimg
                random.choice(_UOM),
            )
            for i in range(n)
        ])

        # 19. BKPF — FI Document Header
        ins("bkpf", [
            (
                m,
                bukrs_pool[i],                          # FK → t001
                belnr_pool[i],
                gjahr,
                random.choice([e.value for e in FIDocumentType]),
                _rand_date(2024, 2024),                 # bldat
                _rand_date(2024, 2024),                 # budat
                f"USER{random.randint(1, 50):03d}",
                random.choice(_TCODES),
                f"Document {belnr_pool[i]}",            # bktxt
                None,                                   # stblg
                None,                                   # stjah
            )
            for i in range(n)
        ])

        # 20. BSEG — FI Document Item
        # Two balanced lines per document (debit "001" + credit "002") → 2 × N rows.
        bseg_rows: list[tuple] = []
        for i in range(n):
            bukrs  = bukrs_pool[i]
            belnr  = belnr_pool[i]
            amount = _rand_amount(100.0, 50_000.0)
            waers  = random.choice(_CURRENCIES)
            dr_acct = saknr_pool[i % n]
            cr_acct = saknr_pool[(i + 1) % n]
            bseg_rows += [
                (                                       # Debit line
                    m, bukrs, belnr, gjahr, "001",
                    PostingKey.DEBIT.value, "S",
                    dr_acct,
                    amount, amount, waers,
                    "Debit posting",
                    None, None,
                    random.choice(_COST_CENTRES),
                    None,
                ),
                (                                       # Credit line
                    m, bukrs, belnr, gjahr, "002",
                    PostingKey.CREDIT.value, "S",
                    cr_acct,
                    amount, amount, waers,
                    "Credit posting",
                    None, None,
                    random.choice(_COST_CENTRES),
                    None,
                ),
            ]
        ins("bseg", bseg_rows)

        self.conn.commit()
        print(
            f"Seeded {n} rows into each table "
            f"({2 * n} rows in bseg — 2 lines per FI document) "
            f"→ {self.db_path}"
        )

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db = Path("sap.db")
    gen = SapGenerator(db_path=str(db), n=10)
    gen.seed()
    gen.close()
    print(f"Done. Database: {db.resolve()}")
