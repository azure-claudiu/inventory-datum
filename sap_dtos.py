"""
SAP DTO entities modeled after common SAP tables.

Modules covered:
  - Basis:     Client (MANDT), Company Code (T001), Plant (T001W)
  - MM:        Material Master (MARA, MARC, MARD), Vendor (LFA1, LFB1)
               Purchase Order header/item (EKKO, EKPO)
  - SD:        Customer (KNA1, KNB1), Sales Order header/item (VBAK, VBAP)
               Delivery header/item (LIKP, LIPS)
  - FI:        G/L Account (SKA1, SKB1), FI Document header/item (BKPF, BSEG)

Naming follows SAP conventions where recognizable (MANDT, BUKRS, WERKS, …).
All entities are plain dataclasses — swap in Pydantic BaseModel if you need
validation / serialisation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class MaterialType(str, Enum):
    RAW_MATERIAL   = "ROH"   # Rohstoff
    FINISHED_GOOD  = "FERT"  # Fertigerzeugnis
    SEMI_FINISHED  = "HALB"  # Halbfabrikat
    TRADING_GOOD   = "HAWA"  # Handelsware
    SERVICE        = "DIEN"  # Dienstleistung
    NON_STOCK      = "NLAG"  # Nicht-Lagerartikel


class PurchaseOrderType(str, Enum):
    STANDARD        = "NB"
    SUBCONTRACTING  = "0001"
    CONSIGNMENT     = "0002"
    STOCK_TRANSFER  = "UB"


class SalesDocumentType(str, Enum):
    STANDARD_ORDER  = "TA"
    RUSH_ORDER      = "SO"
    RETURNS         = "RE"
    CREDIT_MEMO_REQ = "CR"
    DEBIT_MEMO_REQ  = "DR"


class DeliveryType(str, Enum):
    OUTBOUND        = "LF"
    RETURNS         = "LR"
    REPLENISHMENT   = "NL"


class FIDocumentType(str, Enum):
    VENDOR_INVOICE  = "KR"
    CUSTOMER_INVOICE= "DR"
    GENERAL_LEDGER  = "SA"
    PAYMENT         = "ZP"
    REVERSAL        = "AB"


class PostingKey(str, Enum):
    DEBIT  = "40"
    CREDIT = "50"
    VENDOR_CREDIT = "31"
    VENDOR_DEBIT  = "21"
    CUSTOMER_DEBIT  = "01"
    CUSTOMER_CREDIT = "11"


# ---------------------------------------------------------------------------
# Basis / Organisational structures
# ---------------------------------------------------------------------------

@dataclass
class CompanyCode:
    """T001 — Buchungskreise (Company Codes)"""
    mandt: str          # Client (3 chars)
    bukrs: str          # Company Code (4 chars)
    butxt: str          # Company Name
    ort01: str          # City
    land1: str          # Country key  (e.g. "DE", "US")
    waers: str          # Currency key (e.g. "EUR", "USD")
    spras: str          # Language key


@dataclass
class Plant:
    """T001W — Plants / Branches"""
    mandt: str
    werks: str          # Plant key (4 chars)
    name1: str          # Plant name
    bukrs: str          # Assigned Company Code  →  FK CompanyCode.bukrs
    land1: str          # Country
    regio: str          # Region / State
    stras: str          # Street address
    pstlz: str          # Postal code
    ort01: str          # City


@dataclass
class StorageLocation:
    """T001L — Storage Locations"""
    mandt: str
    werks: str          # Plant  →  FK Plant.werks
    lgort: str          # Storage Location key (4 chars)
    lgobe: str          # Description


# ---------------------------------------------------------------------------
# MM — Material Master
# ---------------------------------------------------------------------------

@dataclass
class MaterialMaster:
    """MARA — General Material Data (client level)"""
    mandt: str
    matnr: str          # Material Number (up to 18 chars)
    ersda: date         # Creation date
    ernam: str          # Created by (username)
    mtart: MaterialType # Material Type
    matkl: str          # Material Group
    meins: str          # Base Unit of Measure  (e.g. "EA", "KG", "L")
    brgew: Decimal      # Gross weight
    ntgew: Decimal      # Net weight
    gewei: str          # Weight unit  (e.g. "KG")
    volum: Decimal      # Volume
    voleh: str          # Volume unit  (e.g. "L")
    mfrpn: Optional[str] = None   # Manufacturer part number
    ean11: Optional[str] = None   # EAN / UPC


@dataclass
class MaterialPlantData:
    """MARC — Material Master: Plant Data"""
    mandt: str
    matnr: str          # →  FK MaterialMaster.matnr
    werks: str          # →  FK Plant.werks
    pstat: str          # Maintenance status
    lvorm: str          # Deletion flag at plant level ("X" = flagged)
    beskz: str          # Procurement type  ("E"=in-house, "F"=external, "X"=both)
    sobsl: Optional[str] = None   # Special procurement key
    minbe: Decimal = Decimal("0") # Reorder point
    eisbe: Decimal = Decimal("0") # Safety stock
    mtvfp: str = "02"  # Checking rule for availability


@dataclass
class MaterialStorageData:
    """MARD — Material Master: Storage Location Data"""
    mandt: str
    matnr: str          # →  FK MaterialMaster.matnr
    werks: str          # →  FK Plant.werks
    lgort: str          # →  FK StorageLocation.lgort
    labst: Decimal      # Unrestricted-use stock
    umlme: Decimal      # Stock in transfer
    insme: Decimal      # Quality inspection stock
    speme: Decimal      # Blocked stock
    einme: Decimal      # Total stock (restricted-use)


# ---------------------------------------------------------------------------
# MM — Vendor
# ---------------------------------------------------------------------------

@dataclass
class Vendor:
    """LFA1 — Vendor Master: General Data"""
    mandt: str
    lifnr: str          # Vendor Account Number
    land1: str          # Country
    name1: str          # Name 1
    name2: Optional[str]
    ort01: str          # City
    pstlz: str          # Postal code
    stras: str          # Street
    telf1: Optional[str]  # Telephone
    ktokk: str          # Account group


@dataclass
class VendorCompanyData:
    """LFB1 — Vendor Master: Company Code Data"""
    mandt: str
    lifnr: str          # →  FK Vendor.lifnr
    bukrs: str          # →  FK CompanyCode.bukrs
    akont: str          # Reconciliation account in G/L
    zterm: str          # Payment terms key
    zwels: str          # Payment methods  (e.g. "CT" = check/transfer)


# ---------------------------------------------------------------------------
# MM — Purchase Order
# ---------------------------------------------------------------------------

@dataclass
class PurchaseOrderHeader:
    """EKKO — Purchase Order Header"""
    mandt: str
    ebeln: str          # Purchase Order Number (10 chars)
    bukrs: str          # →  FK CompanyCode.bukrs
    bstyp: str          # PO category  ("F" = PO, "K" = contract, "L" = scheduling)
    bsart: PurchaseOrderType
    lifnr: str          # →  FK Vendor.lifnr
    ekgrp: str          # Purchasing group (3 chars)
    ekorg: str          # Purchasing organisation (4 chars)
    waers: str          # Currency
    wkurs: Decimal      # Exchange rate
    bedat: date         # Purchase order date
    kdatb: Optional[date] = None  # Start of validity period
    kdate: Optional[date] = None  # End of validity period


@dataclass
class PurchaseOrderItem:
    """EKPO — Purchase Order Item"""
    mandt: str
    ebeln: str          # →  FK PurchaseOrderHeader.ebeln
    ebelp: str          # Item number (5 chars, zero-padded, e.g. "00010")
    matnr: str          # →  FK MaterialMaster.matnr
    werks: str          # →  FK Plant.werks
    lgort: Optional[str]  # →  FK StorageLocation.lgort
    menge: Decimal      # Order quantity
    meins: str          # Unit of measure
    netpr: Decimal      # Net price
    peinh: Decimal      # Price unit  (qty to which price applies)
    eindt: date         # Delivery date
    elikz: str          # Delivery completed flag  ("X" or "")
    erekz: str          # Final invoice flag       ("X" or "")


# ---------------------------------------------------------------------------
# SD — Customer
# ---------------------------------------------------------------------------

@dataclass
class Customer:
    """KNA1 — Customer Master: General Data"""
    mandt: str
    kunnr: str          # Customer Account Number
    land1: str          # Country
    name1: str
    name2: Optional[str]
    ort01: str          # City
    pstlz: str          # Postal code
    stras: str          # Street
    telf1: Optional[str]
    ktokd: str          # Customer account group


@dataclass
class CustomerCompanyData:
    """KNB1 — Customer Master: Company Code Data"""
    mandt: str
    kunnr: str          # →  FK Customer.kunnr
    bukrs: str          # →  FK CompanyCode.bukrs
    akont: str          # Reconciliation G/L account
    zterm: str          # Payment terms


# ---------------------------------------------------------------------------
# SD — Sales Order
# ---------------------------------------------------------------------------

@dataclass
class SalesOrderHeader:
    """VBAK — Sales Document Header"""
    mandt: str
    vbeln: str          # Sales Document Number (10 chars)
    auart: SalesDocumentType
    audat: date         # Document date
    vkorg: str          # Sales organisation (4 chars)
    vtweg: str          # Distribution channel (2 chars)
    spart: str          # Division (2 chars)
    kunnr: str          # →  FK Customer.kunnr  (sold-to party)
    waerk: str          # Document currency
    netwr: Decimal      # Net value
    knumv: str          # Condition record number (pricing)


@dataclass
class SalesOrderItem:
    """VBAP — Sales Document Item"""
    mandt: str
    vbeln: str          # →  FK SalesOrderHeader.vbeln
    posnr: str          # Item number (6 chars, e.g. "000010")
    matnr: str          # →  FK MaterialMaster.matnr
    werks: str          # →  FK Plant.werks
    menge: Decimal      # Order quantity
    meins: str          # Unit of measure
    netwr: Decimal      # Net value (item)
    netpr: Decimal      # Net price
    pmatn: Optional[str] = None  # Pricing reference material
    abgru: Optional[str] = None  # Rejection reason code


# ---------------------------------------------------------------------------
# SD — Delivery
# ---------------------------------------------------------------------------

@dataclass
class DeliveryHeader:
    """LIKP — Delivery Header"""
    mandt: str
    vbeln: str          # Delivery Number
    lfart: DeliveryType
    wadat: date         # Planned goods movement date
    werks: str          # Shipping plant  →  FK Plant.werks
    kunnr: str          # Ship-to party   →  FK Customer.kunnr
    anzpk: int          # Number of packages


@dataclass
class DeliveryItem:
    """LIPS — Delivery Item"""
    mandt: str
    vbeln: str          # →  FK DeliveryHeader.vbeln
    posnr: str          # Item number
    matnr: str          # →  FK MaterialMaster.matnr
    werks: str          # Plant
    lgort: str          # Storage location
    lfimg: Decimal      # Actual quantity delivered
    vrkme: str          # Sales unit


# ---------------------------------------------------------------------------
# FI — G/L Account
# ---------------------------------------------------------------------------

@dataclass
class GLAccountMaster:
    """SKA1 — G/L Account Master: Chart of Accounts"""
    mandt: str
    ktopl: str          # Chart of accounts (4 chars)
    saknr: str          # G/L Account Number
    ktoks: str          # Account group
    xbilk: str          # Balance sheet account indicator  ("X" or "")
    gvtyp: Optional[str]  # P&L statement account type
    txt20: str          # Short description
    txt50: str          # Long description


@dataclass
class GLAccountCompanyData:
    """SKB1 — G/L Account Master: Company Code Data"""
    mandt: str
    bukrs: str          # →  FK CompanyCode.bukrs
    saknr: str          # →  FK GLAccountMaster.saknr
    xspeb: str          # Blocked for posting  ("X" or "")
    xopvw: str          # Open item management  ("X" or "")
    xkres: str          # Line item display      ("X" or "")
    waers: str          # Account currency


# ---------------------------------------------------------------------------
# FI — Document
# ---------------------------------------------------------------------------

@dataclass
class FIDocumentHeader:
    """BKPF — Accounting Document Header"""
    mandt: str
    bukrs: str          # →  FK CompanyCode.bukrs
    belnr: str          # Document number (10 chars)
    gjahr: str          # Fiscal year (4 chars)
    blart: FIDocumentType
    bldat: date         # Document date
    budat: date         # Posting date
    usnam: str          # Username of person who posted
    tcode: str          # Transaction code used for posting
    bktxt: Optional[str] = None  # Document header text
    stblg: Optional[str] = None  # Reverse document number
    stjah: Optional[str] = None  # Reverse document fiscal year


@dataclass
class FIDocumentItem:
    """BSEG — Accounting Document Segment (Line Item)"""
    mandt: str
    bukrs: str          # →  FK FIDocumentHeader.bukrs
    belnr: str          # →  FK FIDocumentHeader.belnr
    gjahr: str          # →  FK FIDocumentHeader.gjahr
    buzei: str          # Line item number (3 chars, e.g. "001")
    bschl: PostingKey   # Posting key
    koart: str          # Account type  ("D"=customer, "K"=vendor, "S"=G/L)
    hkont: str          # G/L account  →  FK GLAccountMaster.saknr
    wrbtr: Decimal      # Amount in document currency
    dmbtr: Decimal      # Amount in local currency
    waers: str          # Document currency
    sgtxt: Optional[str] = None  # Item text
    lifnr: Optional[str] = None  # Vendor  →  FK Vendor.lifnr   (if koart="K")
    kunnr: Optional[str] = None  # Customer →  FK Customer.kunnr (if koart="D")
    kostl: Optional[str] = None  # Cost centre
    aufnr: Optional[str] = None  # Internal order number
