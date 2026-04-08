#!/usr/bin/env python3
"""
Congressional Stock Trade Disclosure Collector
===============================================
Collects U.S. politician stock trade disclosures from official government
sources and stores them in a local SQLite database.

Sources:
  1. House Clerk XML index + PTR PDFs
     https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.xml
  2. Senate eFD (Electronic Financial Disclosures)
     https://efdsearch.senate.gov/search/

No paid APIs required. Run on your own machine whenever you want.

Usage:
    python congress_trades.py                  # Run once now
    python congress_trades.py --schedule 6     # Run every 6 hours
    python congress_trades.py --years 2025 2026
    python congress_trades.py --house-only
    python congress_trades.py --senate-only
    python congress_trades.py --export csv     # Export DB to CSV
    python congress_trades.py --query NVDA     # Search trades by ticker
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# Optional imports – installed by requirements.txt
# ---------------------------------------------------------------------------
try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print(
        "Missing dependencies. Install them with:\n"
        "  pip install -r requirements.txt\n"
    )
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    pdfplumber = None  # PDF parsing will be skipped with a warning

try:
    import schedule as schedule_lib
except ImportError:
    schedule_lib = None  # Scheduling will fall back to time.sleep loop

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress_trades.db"
LOG_PATH = BASE_DIR / "congress_trades.log"

HOUSE_XML_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.xml"
HOUSE_PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

SENATE_SEARCH_URL = "https://efdsearch.senate.gov/search/"
SENATE_SEARCH_REPORT_URL = "https://efdsearch.senate.gov/search/report/data/"
SENATE_VIEW_BASE = "https://efdsearch.senate.gov"
SENATE_AGREE_URL = "https://efdsearch.senate.gov/search/home/"

# Filing types that contain stock transactions (Periodic Transaction Reports)
HOUSE_PTR_FILING_TYPES = {"P"}  # P = Periodic Transaction Report

# Rate limiting – be respectful to government servers
REQUEST_DELAY = 1.5        # seconds between requests
SENATE_REQUEST_DELAY = 2.0  # senate is stricter
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# Current year for default operation
CURRENT_YEAR = datetime.now().year

# Amount range mapping for standardization
AMOUNT_RANGES = {
    "$1,001 - $15,000": (1001, 15000),
    "$15,001 - $50,000": (15001, 50000),
    "$50,001 - $100,000": (50001, 100000),
    "$100,001 - $250,000": (100001, 250000),
    "$250,001 - $500,000": (250001, 500000),
    "$500,001 - $1,000,000": (500001, 1000000),
    "$1,000,001 - $5,000,000": (1000001, 5000000),
    "$5,000,001 - $25,000,000": (5000001, 25000000),
    "$25,000,001 - $50,000,000": (25000001, 50000000),
    "$50,000,001 -": (50000001, None),
    "Over $50,000,000": (50000001, None),
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create the SQLite database and tables if they don't exist."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            hash            TEXT UNIQUE NOT NULL,
            politician      TEXT NOT NULL,
            chamber         TEXT,           -- 'House' or 'Senate'
            party           TEXT,
            state           TEXT,
            district        TEXT,
            trade_date      TEXT,           -- ISO format YYYY-MM-DD
            filing_date     TEXT,           -- ISO format YYYY-MM-DD
            ticker          TEXT,
            asset_name      TEXT,
            trade_type      TEXT,           -- 'buy', 'sell', 'exchange'
            amount_low      INTEGER,
            amount_high     INTEGER,
            amount_raw      TEXT,           -- original string
            owner           TEXT,           -- 'Self', 'Spouse', 'Child', 'Joint'
            description     TEXT,           -- footnotes / comments
            doc_id          TEXT,           -- source document ID
            source          TEXT,           -- 'house_clerk' or 'senate_efd'
            source_url      TEXT,
            collected_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_trades_politician ON trades(politician);
        CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);
        CREATE INDEX IF NOT EXISTS idx_trades_trade_date ON trades(trade_date);
        CREATE INDEX IF NOT EXISTS idx_trades_hash ON trades(hash);

        CREATE TABLE IF NOT EXISTS collection_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT NOT NULL,
            year        INTEGER,
            started_at  TEXT,
            finished_at TEXT,
            records_found   INTEGER DEFAULT 0,
            records_new     INTEGER DEFAULT 0,
            status      TEXT DEFAULT 'running'
        );
    """)
    conn.commit()
    return conn


def trade_hash(politician: str, trade_date: str, ticker: str, trade_type: str,
               amount_raw: str, doc_id: str) -> str:
    """Generate a unique hash for deduplication."""
    raw = f"{politician}|{trade_date}|{ticker}|{trade_type}|{amount_raw}|{doc_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def insert_trade(conn: sqlite3.Connection, trade: dict) -> bool:
    """Insert a trade record, return True if new (not duplicate)."""
    h = trade_hash(
        trade.get("politician", ""),
        trade.get("trade_date", ""),
        trade.get("ticker", ""),
        trade.get("trade_type", ""),
        trade.get("amount_raw", ""),
        trade.get("doc_id", ""),
    )
    trade["hash"] = h

    # Parse amount range
    amount_low, amount_high = parse_amount(trade.get("amount_raw", ""))
    trade["amount_low"] = amount_low
    trade["amount_high"] = amount_high

    try:
        conn.execute("""
            INSERT INTO trades (
                hash, politician, chamber, party, state, district,
                trade_date, filing_date, ticker, asset_name,
                trade_type, amount_low, amount_high, amount_raw,
                owner, description, doc_id, source, source_url
            ) VALUES (
                :hash, :politician, :chamber, :party, :state, :district,
                :trade_date, :filing_date, :ticker, :asset_name,
                :trade_type, :amount_low, :amount_high, :amount_raw,
                :owner, :description, :doc_id, :source, :source_url
            )
        """, trade)
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # Duplicate


def parse_amount(raw: str) -> tuple:
    """Parse amount range string into (low, high) integers."""
    if not raw:
        return (None, None)
    raw = raw.strip()

    # Direct lookup
    for pattern, (low, high) in AMOUNT_RANGES.items():
        if pattern.lower() in raw.lower():
            return (low, high)

    # Try regex for "$X - $Y" or "$X-$Y" patterns
    m = re.search(r'\$?([\d,]+)\s*[-–]\s*\$?([\d,]+)', raw)
    if m:
        low = int(m.group(1).replace(",", ""))
        high = int(m.group(2).replace(",", ""))
        return (low, high)

    # Single value
    m = re.search(r'\$?([\d,]+)', raw)
    if m:
        val = int(m.group(1).replace(",", ""))
        return (val, val)

    return (None, None)


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "CongressTradeCollector/1.0 "
            "(Personal research; https://github.com; respects rate limits)"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


def fetch_with_retry(session: requests.Session, url: str,
                     delay: float = REQUEST_DELAY, **kwargs) -> Optional[requests.Response]:
    """Fetch URL with retries and rate limiting."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(delay)
            resp = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {wait}s before retry...")
                time.sleep(wait)
            elif resp.status_code == 403:
                logger.warning(f"403 Forbidden: {url} (attempt {attempt})")
                time.sleep(delay * 3)
            else:
                logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error for {url}: {e} (attempt {attempt})")
            time.sleep(delay * 2)
    logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
    return None


# ---------------------------------------------------------------------------
# Source 1: House Clerk XML + PDF
# ---------------------------------------------------------------------------
def collect_house(conn: sqlite3.Connection, session: requests.Session,
                  years: list[int] = None):
    """Collect House PTR filings from the Clerk's XML index + PDF parsing."""
    if years is None:
        years = [CURRENT_YEAR - 1, CURRENT_YEAR]

    for year in years:
        log_id = start_collection_log(conn, "house_clerk", year)
        found = 0
        new = 0

        logger.info(f"--- House Clerk: Fetching {year} XML index ---")
        xml_url = HOUSE_XML_URL.format(year=year)
        resp = fetch_with_retry(session, xml_url)
        if not resp:
            finish_collection_log(conn, log_id, found, new, "error")
            continue

        # Parse the XML index
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.error(f"XML parse error for {year}: {e}")
            finish_collection_log(conn, log_id, found, new, "error")
            continue

        members = root.findall(".//Member")
        ptr_filings = [
            m for m in members
            if m.findtext("FilingType", "").strip() in HOUSE_PTR_FILING_TYPES
        ]
        logger.info(f"  Found {len(ptr_filings)} PTR filings out of {len(members)} total")

        for member in ptr_filings:
            doc_id = member.findtext("DocID", "").strip()
            if not doc_id:
                continue

            first = member.findtext("First", "").strip()
            last = member.findtext("Last", "").strip()
            prefix = member.findtext("Prefix", "").strip()
            suffix = member.findtext("Suffix", "").strip()
            state_dst = member.findtext("StateDst", "").strip()
            filing_date = member.findtext("FilingDate", "").strip()

            politician = f"{first} {last}".strip()
            if suffix:
                politician += f" {suffix}"

            # Parse state/district
            state = state_dst[:2] if len(state_dst) >= 2 else state_dst
            district = state_dst[2:] if len(state_dst) > 2 else None

            # Normalize filing date
            filing_date_iso = normalize_date(filing_date)

            # Check if we already have trades from this doc
            existing = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE doc_id = ? AND source = 'house_clerk'",
                (doc_id,)
            ).fetchone()[0]
            if existing > 0:
                continue

            # Fetch and parse the PTR PDF
            pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
            trades = parse_house_ptr(session, pdf_url, politician, state, district,
                                     filing_date_iso, doc_id)
            found += len(trades)

            for t in trades:
                if insert_trade(conn, t):
                    new += 1

            if trades:
                logger.info(f"  {politician} ({state_dst}): {len(trades)} trades from doc {doc_id}")

        finish_collection_log(conn, log_id, found, new, "complete")
        logger.info(f"  House {year} complete: {found} found, {new} new")


def parse_house_ptr(session: requests.Session, pdf_url: str,
                    politician: str, state: str, district: str,
                    filing_date: str, doc_id: str) -> list[dict]:
    """Download and parse a House PTR PDF into trade records."""
    if pdfplumber is None:
        # Fall back to text-based extraction from the raw PDF response
        return parse_house_ptr_text(session, pdf_url, politician, state,
                                     district, filing_date, doc_id)

    resp = fetch_with_retry(session, pdf_url)
    if not resp:
        return []

    trades = []
    try:
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            # Also try table extraction
            all_tables = []
            for page in pdf.pages:
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)

            if all_tables:
                trades = parse_ptr_tables(all_tables, politician, state, district,
                                          filing_date, doc_id, pdf_url)
            if not trades:
                # Fall back to text-based parsing
                trades = parse_ptr_text_content(full_text, politician, state, district,
                                                 filing_date, doc_id, pdf_url)
    except Exception as e:
        logger.debug(f"PDF parse error for {pdf_url}: {e}")

    return trades


def parse_house_ptr_text(session: requests.Session, pdf_url: str,
                         politician: str, state: str, district: str,
                         filing_date: str, doc_id: str) -> list[dict]:
    """Minimal PDF text extraction without pdfplumber (fetch_url-based)."""
    # Without pdfplumber we can't parse PDFs – return empty
    logger.debug(f"Skipping PDF {doc_id} (pdfplumber not installed)")
    return []


def parse_ptr_tables(tables: list, politician: str, state: str, district: str,
                     filing_date: str, doc_id: str, source_url: str) -> list[dict]:
    """Parse structured table data extracted from PTR PDFs."""
    trades = []

    for table in tables:
        if not table or len(table) < 2:
            continue

        # Find the header row
        header = None
        data_start = 0
        for i, row in enumerate(table):
            row_text = " ".join(str(cell or "") for cell in row).lower()
            if "asset" in row_text and ("transaction" in row_text or "type" in row_text):
                header = row
                data_start = i + 1
                break

        if not header:
            continue

        # Map column positions
        col_map = {}
        for j, cell in enumerate(header):
            cell_text = str(cell or "").lower().strip()
            if "asset" in cell_text:
                col_map["asset"] = j
            elif "type" in cell_text and "transaction" in cell_text:
                col_map["type"] = j
            elif "date" in cell_text and "notification" not in cell_text:
                col_map["date"] = j
            elif "amount" in cell_text:
                col_map["amount"] = j
            elif "owner" in cell_text:
                col_map["owner"] = j

        for row in table[data_start:]:
            if not row or all(not cell for cell in row):
                continue

            asset_text = str(row[col_map.get("asset", 0)] or "")
            if not asset_text or len(asset_text) < 3:
                continue

            ticker = extract_ticker(asset_text)
            trade_type_raw = str(row[col_map.get("type", 1)] or "").strip()
            trade_date_raw = str(row[col_map.get("date", 2)] or "").strip()
            amount_raw = str(row[col_map.get("amount", 3)] or "").strip()
            owner_raw = str(row[col_map.get("owner", -1)] or "").strip() if "owner" in col_map else ""

            trade_type = normalize_trade_type(trade_type_raw)
            trade_date = normalize_date(trade_date_raw)
            owner = normalize_owner(owner_raw)

            if not trade_type:
                continue

            trades.append({
                "politician": politician,
                "chamber": "House",
                "party": None,
                "state": state,
                "district": district,
                "trade_date": trade_date,
                "filing_date": filing_date,
                "ticker": ticker,
                "asset_name": clean_asset_name(asset_text),
                "trade_type": trade_type,
                "amount_raw": amount_raw,
                "owner": owner,
                "description": None,
                "doc_id": doc_id,
                "source": "house_clerk",
                "source_url": source_url,
            })

    return trades


def parse_ptr_text_content(text: str, politician: str, state: str, district: str,
                           filing_date: str, doc_id: str, source_url: str) -> list[dict]:
    """Parse PTR content from raw text when table extraction fails."""
    trades = []

    # Look for transaction patterns in the text
    # Common format: "Asset Name (TICKER) [ST]    P    01/16/2026    $1,000,001 - $5,000,000"
    lines = text.split("\n")

    # Track multi-line transaction parsing
    current_asset = None
    current_type = None
    current_date = None
    current_amount = None
    current_owner = None
    current_description = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Try to detect transaction lines
        # Pattern: contains ticker in parens + transaction type + date + amount range
        ticker_match = re.search(r'\(([A-Z]{1,5})\)', line)
        type_match = re.search(r'\b(P|S|S \(partial\)|E)\b', line)
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', line)
        amount_match = re.search(r'(\$[\d,]+\s*[-–]\s*\$[\d,]+)', line)

        # Check for owner prefix (SP = Spouse, JT = Joint, DC = Dependent Child)
        owner_match = re.match(r'^(SP|JT|DC|Self)\s', line)

        if ticker_match and (type_match or amount_match):
            ticker = ticker_match.group(1)
            asset_name = line[:ticker_match.start()].strip()

            trade_type_raw = type_match.group(1) if type_match else ""
            trade_date_raw = date_match.group(1) if date_match else ""
            amount_raw = amount_match.group(1) if amount_match else ""
            owner = ""

            if owner_match:
                owner = normalize_owner(owner_match.group(1))

            trade_type = normalize_trade_type(trade_type_raw)
            trade_date = normalize_date(trade_date_raw)

            if trade_type:
                trades.append({
                    "politician": politician,
                    "chamber": "House",
                    "party": None,
                    "state": state,
                    "district": district,
                    "trade_date": trade_date,
                    "filing_date": filing_date,
                    "ticker": ticker,
                    "asset_name": clean_asset_name(asset_name),
                    "trade_type": trade_type,
                    "amount_raw": amount_raw,
                    "owner": owner,
                    "description": None,
                    "doc_id": doc_id,
                    "source": "house_clerk",
                    "source_url": source_url,
                })

    return trades


# ---------------------------------------------------------------------------
# Source 2: Senate eFD
# ---------------------------------------------------------------------------
def _senate_datatables_payload(start: int = 0, length: int = 100,
                                 year_start: str = "",
                                 year_end: str = "") -> dict:
    """Build the DataTables POST body that eFD expects."""
    payload = {
        "draw": "1",
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "report_types": "11",  # 11 = Periodic Transaction Report
        "filer_types": "",
        "submitted_start_date": year_start,
        "submitted_end_date": year_end,
        "candidate_state": "",
        "senator_state": "",
        "office_id": "",
        "first_name": "",
        "last_name": "",
        "order[0][column]": "1",
        "order[0][dir]": "desc",
    }
    for i in range(5):
        payload[f"columns[{i}][data]"] = str(i)
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = "true"
        payload[f"columns[{i}][orderable]"] = "true"
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = "false"
    return payload


def collect_senate(conn: sqlite3.Connection, session: requests.Session,
                   years: list[int] = None):
    """Collect Senate PTR filings from the eFD search system.

    Uses the DataTables JSON API behind efdsearch.senate.gov:
      1. GET /search/ to establish session + CSRF cookie
      2. POST /search/home/ to accept the usage agreement
      3. POST /search/report/data/ with DataTables params to list PTR filings
      4. GET each individual PTR page and parse the HTML trade table

    The eFD server has intermittent availability (503 errors are common,
    especially on weekends). The collector retries with backoff.
    """
    if years is None:
        years = [CURRENT_YEAR - 1, CURRENT_YEAR]

    for year in years:
        log_id = start_collection_log(conn, "senate_efd", year)
        found = 0
        new = 0

        logger.info(f"--- Senate eFD: Collecting {year} ---")

        # Step 1: GET search page to get CSRF cookie
        try:
            resp = session.get(SENATE_SEARCH_URL, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not reach Senate eFD: {e}")
            finish_collection_log(conn, log_id, found, new, "error")
            continue

        if resp.status_code != 200:
            logger.warning(f"Senate eFD returned {resp.status_code}")
            finish_collection_log(conn, log_id, found, new, "error")
            continue

        csrf = session.cookies.get("csrftoken", "")
        if not csrf:
            # Fallback: try to find it in an HTML form input
            soup = BeautifulSoup(resp.text, "html.parser")
            inp = soup.find("input", {"name": "csrfmiddlewaretoken"})
            csrf = inp["value"] if inp else ""
        if not csrf:
            logger.warning("Could not obtain CSRF token from Senate eFD")
            finish_collection_log(conn, log_id, found, new, "error")
            continue

        # Step 2: Accept the terms-of-use agreement
        time.sleep(SENATE_REQUEST_DELAY)
        try:
            agree_resp = session.post(
                SENATE_AGREE_URL,
                data={
                    "csrfmiddlewaretoken": csrf,
                    "prohibition_agreement": "1",
                },
                headers={
                    "Referer": SENATE_SEARCH_URL,
                    "X-CSRFToken": csrf,
                },
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            logger.info(f"  Agreement POST: {agree_resp.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"  Agreement POST failed: {e}")
            finish_collection_log(conn, log_id, found, new, "error")
            continue

        # Step 3: Paginate through the DataTables JSON endpoint
        filings = []
        page_start = 0
        page_size = 100
        year_start = f"01/01/{year} 00:00:00"
        year_end = f"12/31/{year} 23:59:59"
        max_pages = 50  # Safety limit

        for page_num in range(max_pages):
            time.sleep(SENATE_REQUEST_DELAY)
            payload = _senate_datatables_payload(
                start=page_start, length=page_size,
                year_start=year_start, year_end=year_end,
            )
            headers = {
                "X-CSRFToken": csrf,
                "Referer": SENATE_SEARCH_URL,
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            }

            # Retry up to 3 times for 503 errors
            data_resp = None
            for attempt in range(3):
                try:
                    data_resp = session.post(
                        SENATE_SEARCH_REPORT_URL,
                        data=payload, headers=headers,
                        timeout=REQUEST_TIMEOUT,
                    )
                    if data_resp.status_code == 200:
                        break
                    if data_resp.status_code == 503:
                        wait = (attempt + 1) * 5
                        logger.info(f"  503 from eFD, retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        logger.warning(f"  Unexpected status {data_resp.status_code}")
                        break
                except requests.exceptions.RequestException as e:
                    logger.warning(f"  Request failed: {e}")
                    time.sleep(5)

            if not data_resp or data_resp.status_code != 200:
                if page_num == 0:
                    logger.warning(
                        f"  Senate eFD data endpoint unavailable for {year}. "
                        f"The server may be under maintenance — try again later."
                    )
                break

            # Parse the DataTables JSON response
            try:
                jdata = data_resp.json()
            except (json.JSONDecodeError, ValueError):
                logger.warning("  Could not parse JSON from eFD data endpoint")
                break

            page_filings = parse_senate_search_results(data_resp.text)
            filings.extend(page_filings)

            total_records = int(jdata.get("recordsFiltered", 0))
            page_start += page_size
            if page_start >= total_records:
                break

            logger.info(
                f"  Page {page_num + 1}: {len(page_filings)} filings "
                f"({page_start}/{total_records})"
            )

        if not filings:
            logger.warning(
                f"  Senate eFD returned no PTR filings for {year}. "
                f"The server has intermittent availability — try again later."
            )
            finish_collection_log(conn, log_id, found, new, "blocked")
            continue

        logger.info(f"  Found {len(filings)} PTR filings for {year}")

        for filing in filings:
            ptr_url = filing.get("url", "")
            doc_id = filing.get("doc_id", "")
            senator = filing.get("politician", "Unknown")
            filed = filing.get("filing_date", "")

            if not ptr_url:
                continue

            # Check if already collected
            existing = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE doc_id = ? AND source = 'senate_efd'",
                (doc_id,)
            ).fetchone()[0]
            if existing > 0:
                continue

            # Fetch and parse the PTR page
            full_url = urljoin(SENATE_VIEW_BASE, ptr_url)
            trades = parse_senate_ptr(session, full_url, senator, filed, doc_id)
            found += len(trades)

            for t in trades:
                if insert_trade(conn, t):
                    new += 1

            if trades:
                logger.info(f"  {senator}: {len(trades)} trades from {doc_id}")

        finish_collection_log(conn, log_id, found, new, "complete")
        logger.info(f"  Senate {year} complete: {found} found, {new} new")


def parse_senate_search_results(html: str) -> list[dict]:
    """Parse the Senate eFD search results page into filing records."""
    filings = []

    # Try JSON first (the /data endpoint may return JSON)
    try:
        data = json.loads(html)
        if isinstance(data, dict) and "data" in data:
            for row in data["data"]:
                # Each row is a list of HTML fragments
                if len(row) >= 5:
                    # Parse name from first column
                    name_html = row[0] if isinstance(row[0], str) else str(row[0])
                    name_soup = BeautifulSoup(name_html, "html.parser")
                    name = name_soup.get_text(strip=True)

                    # Parse URL from report type column
                    report_html = row[3] if len(row) > 3 else ""
                    report_soup = BeautifulSoup(str(report_html), "html.parser")
                    link = report_soup.find("a")
                    url = link.get("href", "") if link else ""

                    # Parse date
                    date_str = row[4] if len(row) > 4 else ""
                    if isinstance(date_str, str):
                        date_soup = BeautifulSoup(date_str, "html.parser")
                        date_str = date_soup.get_text(strip=True)

                    doc_id = url.split("/")[-2] if url and "/" in url else hashlib.md5(url.encode()).hexdigest()[:12]

                    filings.append({
                        "politician": name,
                        "url": url,
                        "doc_id": doc_id,
                        "filing_date": normalize_date(date_str),
                    })
            return filings
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # Fall back to HTML parsing
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        # Try finding links to PTR reports
        links = soup.find_all("a", href=re.compile(r"/search/view/ptr/"))
        for link in links:
            url = link.get("href", "")
            text = link.get_text(strip=True)
            doc_id = url.split("/")[-2] if "/" in url else ""
            filings.append({
                "politician": text or "Unknown",
                "url": url,
                "doc_id": doc_id,
                "filing_date": None,
            })
        return filings

    rows = table.find_all("tr")[1:]  # Skip header
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        name = cells[0].get_text(strip=True)
        link = cells[3].find("a") if len(cells) > 3 else None
        url = link.get("href", "") if link else ""
        date_str = cells[4].get_text(strip=True) if len(cells) > 4 else ""

        doc_id = url.split("/")[-2] if url and "/" in url else ""

        filings.append({
            "politician": name,
            "url": url,
            "doc_id": doc_id,
            "filing_date": normalize_date(date_str),
        })

    return filings


def parse_senate_ptr(session: requests.Session, url: str,
                     politician: str, filing_date: str,
                     doc_id: str) -> list[dict]:
    """Parse an individual Senate PTR page into trade records."""
    resp = fetch_with_retry(session, url, delay=SENATE_REQUEST_DELAY)
    if not resp:
        return []

    trades = []
    soup = BeautifulSoup(resp.text, "html.parser")

    # Senate PTR pages have a table with transactions
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Check if this is a transaction table
        header_text = " ".join(rows[0].get_text().lower().split())
        if "transaction" not in header_text and "asset" not in header_text:
            continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            # Senate PTR format varies but typically:
            # Transaction Date | Owner | Ticker | Asset Name | Type | Amount | Comment
            cell_texts = [c.get_text(strip=True) for c in cells]

            # Try to identify columns by content
            trade_info = extract_senate_trade_from_cells(cell_texts)
            if not trade_info:
                continue

            trades.append({
                "politician": politician,
                "chamber": "Senate",
                "party": None,
                "state": None,
                "district": None,
                "trade_date": trade_info.get("date"),
                "filing_date": filing_date,
                "ticker": trade_info.get("ticker"),
                "asset_name": trade_info.get("asset_name"),
                "trade_type": trade_info.get("trade_type"),
                "amount_raw": trade_info.get("amount"),
                "owner": trade_info.get("owner"),
                "description": trade_info.get("comment"),
                "doc_id": doc_id,
                "source": "senate_efd",
                "source_url": url,
            })

    return trades


def extract_senate_trade_from_cells(cells: list[str]) -> Optional[dict]:
    """Extract trade information from a row of cell texts."""
    if not cells or len(cells) < 3:
        return None

    result = {
        "date": None,
        "owner": None,
        "ticker": None,
        "asset_name": None,
        "trade_type": None,
        "amount": None,
        "comment": None,
    }

    for cell in cells:
        cell = cell.strip()
        if not cell or cell == "--":
            continue

        # Date detection
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', cell) and not result["date"]:
            result["date"] = normalize_date(cell)
            continue

        # Amount detection
        if "$" in cell and re.search(r'\d', cell):
            result["amount"] = cell
            continue

        # Trade type detection
        cell_lower = cell.lower()
        if cell_lower in ("purchase", "sale", "sale (full)", "sale (partial)", "exchange"):
            result["trade_type"] = normalize_trade_type(cell)
            continue

        # Owner detection
        if cell_lower in ("self", "spouse", "joint", "child", "dependent child"):
            result["owner"] = normalize_owner(cell)
            continue

        # Ticker detection (usually short uppercase)
        if re.match(r'^[A-Z]{1,5}$', cell):
            result["ticker"] = cell
            continue

        # Asset name (longest text that doesn't match other patterns)
        if len(cell) > 5 and not result["asset_name"]:
            result["asset_name"] = cell
            # Try to extract ticker from asset name
            ticker_match = re.search(r'\(([A-Z]{1,5})\)', cell)
            if ticker_match:
                result["ticker"] = ticker_match.group(1)

    # Need at least an asset name or ticker, plus trade type
    if (result["ticker"] or result["asset_name"]) and result["trade_type"]:
        return result
    return None


# ---------------------------------------------------------------------------
# Utility / normalization functions
# ---------------------------------------------------------------------------
# Asset type codes from House filings – these are NOT tickers
HOUSE_ASSET_TYPE_CODES = {
    "ST", "OP", "OI", "CS", "PS", "MF", "EF", "OT", "DC", "FN", "AB",
    "MU", "HN", "RS", "PM", "QR", "NF", "CR", "BK", "ET",
}


def extract_ticker(text: str) -> Optional[str]:
    """Extract stock ticker from asset description text."""
    # Pattern: "Company Name (TICK)" or "(TICK:US)" or "[TICK]"
    # Try ticker with exchange suffix first (most reliable)
    m = re.search(r'\(([A-Z]{1,5}):[A-Z]+\)', text)
    if m:
        return m.group(1)

    # Try parenthetical ticker — but skip if it's an asset type code
    m = re.search(r'\(([A-Z]{1,5})\)', text)
    if m:
        candidate = m.group(1)
        if candidate not in HOUSE_ASSET_TYPE_CODES:
            return candidate

    # Try bracket ticker
    m = re.search(r'\[([A-Z]{1,5})\]', text)
    if m:
        candidate = m.group(1)
        if candidate not in HOUSE_ASSET_TYPE_CODES:
            return candidate

    return None


def clean_asset_name(text: str) -> str:
    """Clean asset description text."""
    # Remove ticker references
    text = re.sub(r'\s*\([A-Z]{1,5}(:[A-Z]+)?\)\s*', ' ', text)
    text = re.sub(r'\s*\[(ST|OP|OI|CS|PS|MF|EF|OT|DC|FN|AB|MU)\]\s*', ' ', text)
    return text.strip()


def normalize_trade_type(raw: str) -> Optional[str]:
    """Normalize trade type to 'buy', 'sell', or 'exchange'."""
    if not raw:
        return None
    raw = raw.strip().lower()

    if raw in ("p", "purchase", "buy"):
        return "buy"
    elif raw in ("s", "s (partial)", "s (full)", "sale", "sale (full)",
                 "sale (partial)", "sell"):
        return "sell"
    elif raw in ("e", "exchange"):
        return "exchange"
    return None


def normalize_owner(raw: str) -> str:
    """Normalize owner field."""
    if not raw:
        return ""
    raw = raw.strip().upper()
    mapping = {
        "SP": "Spouse",
        "JT": "Joint",
        "DC": "Dependent Child",
        "SELF": "Self",
        "SPOUSE": "Spouse",
        "JOINT": "Joint",
        "CHILD": "Dependent Child",
        "DEPENDENT CHILD": "Dependent Child",
    }
    return mapping.get(raw, raw)


def normalize_date(raw: str) -> Optional[str]:
    """Convert various date formats to ISO YYYY-MM-DD."""
    if not raw:
        return None

    raw = raw.strip()
    formats = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw  # Return original if can't parse


# ---------------------------------------------------------------------------
# Collection log helpers
# ---------------------------------------------------------------------------
def start_collection_log(conn: sqlite3.Connection, source: str, year: int) -> int:
    cursor = conn.execute(
        "INSERT INTO collection_log (source, year, started_at, status) VALUES (?, ?, datetime('now'), 'running')",
        (source, year)
    )
    conn.commit()
    return cursor.lastrowid


def finish_collection_log(conn: sqlite3.Connection, log_id: int,
                          found: int, new: int, status: str):
    conn.execute(
        "UPDATE collection_log SET finished_at=datetime('now'), records_found=?, records_new=?, status=? WHERE id=?",
        (found, new, status, log_id)
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Query / Export
# ---------------------------------------------------------------------------
def query_trades(conn: sqlite3.Connection, ticker: str = None,
                 politician: str = None, days: int = None,
                 limit: int = 50) -> list[dict]:
    """Query trades from the database."""
    clauses = []
    params = []

    if ticker:
        clauses.append("ticker LIKE ?")
        params.append(f"%{ticker.upper()}%")
    if politician:
        clauses.append("politician LIKE ?")
        params.append(f"%{politician}%")
    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        clauses.append("trade_date >= ?")
        params.append(cutoff)

    where = " AND ".join(clauses) if clauses else "1=1"
    params.append(limit)

    rows = conn.execute(f"""
        SELECT politician, chamber, party, state, trade_date, filing_date,
               ticker, asset_name, trade_type, amount_raw, amount_low, amount_high,
               owner, doc_id, source, source_url
        FROM trades
        WHERE {where}
        ORDER BY trade_date DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "politician", "chamber", "party", "state", "trade_date", "filing_date",
        "ticker", "asset_name", "trade_type", "amount_raw", "amount_low", "amount_high",
        "owner", "doc_id", "source", "source_url"
    ]
    return [dict(zip(columns, row)) for row in rows]


def export_csv(conn: sqlite3.Connection, output_path: str = None):
    """Export all trades to CSV."""
    if not output_path:
        output_path = str(BASE_DIR / f"congress_trades_export_{datetime.now():%Y%m%d_%H%M%S}.csv")

    rows = conn.execute("""
        SELECT politician, chamber, party, state, district, trade_date, filing_date,
               ticker, asset_name, trade_type, amount_raw, amount_low, amount_high,
               owner, description, doc_id, source, source_url, collected_at
        FROM trades
        ORDER BY trade_date DESC
    """).fetchall()

    columns = [
        "politician", "chamber", "party", "state", "district", "trade_date",
        "filing_date", "ticker", "asset_name", "trade_type", "amount_raw",
        "amount_low", "amount_high", "owner", "description", "doc_id",
        "source", "source_url", "collected_at"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(zip(columns, row)))

    logger.info(f"Exported {len(rows)} trades to {output_path}")
    return output_path


def print_summary(conn: sqlite3.Connection):
    """Print a summary of the database contents."""
    total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    by_source = conn.execute(
        "SELECT source, COUNT(*) FROM trades GROUP BY source"
    ).fetchall()
    by_chamber = conn.execute(
        "SELECT chamber, COUNT(*) FROM trades GROUP BY chamber"
    ).fetchall()
    latest = conn.execute(
        "SELECT MAX(trade_date) FROM trades"
    ).fetchone()[0]
    most_active = conn.execute("""
        SELECT politician, COUNT(*) as cnt
        FROM trades
        GROUP BY politician
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    print(f"\n{'='*60}")
    print(f"  Congressional Trades Database Summary")
    print(f"{'='*60}")
    print(f"  Total trades:    {total:,}")
    print(f"  Latest trade:    {latest or 'N/A'}")
    print(f"\n  By Source:")
    for source, count in by_source:
        print(f"    {source:20s} {count:,}")
    print(f"\n  By Chamber:")
    for chamber, count in by_chamber:
        print(f"    {(chamber or 'Unknown'):20s} {count:,}")
    print(f"\n  Most Active Traders:")
    for name, count in most_active:
        print(f"    {name:30s} {count:,} trades")
    print(f"{'='*60}\n")


def print_query_results(results: list[dict]):
    """Pretty-print query results."""
    if not results:
        print("  No trades found matching your query.")
        return

    print(f"\n  {'Date':<12} {'Politician':<25} {'Type':<6} {'Ticker':<8} {'Amount':<25} {'Source'}")
    print(f"  {'-'*12} {'-'*25} {'-'*6} {'-'*8} {'-'*25} {'-'*12}")
    for t in results:
        date = t.get("trade_date", "")[:10]
        name = (t.get("politician", ""))[:24]
        ttype = (t.get("trade_type", ""))[:5]
        ticker = (t.get("ticker", "") or "N/A")[:7]
        amount = (t.get("amount_raw", "") or "N/A")[:24]
        source = (t.get("source", ""))[:12]
        print(f"  {date:<12} {name:<25} {ttype:<6} {ticker:<8} {amount:<25} {source}")
    print(f"\n  {len(results)} result(s)\n")


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
def run_collection(conn: sqlite3.Connection, session: requests.Session,
                   years: list[int], house: bool = True, senate: bool = True):
    """Run a full collection cycle."""
    logger.info("=" * 50)
    logger.info("Starting collection run")
    logger.info("=" * 50)

    if house:
        try:
            collect_house(conn, session, years)
        except Exception as e:
            logger.error(f"House collection error: {e}", exc_info=True)

    if senate:
        try:
            collect_senate(conn, session, years)
        except Exception as e:
            logger.error(f"Senate collection error: {e}", exc_info=True)

    print_summary(conn)
    logger.info("Collection run complete")


def run_scheduled(interval_hours: int, conn: sqlite3.Connection,
                  session: requests.Session, years: list[int],
                  house: bool = True, senate: bool = True):
    """Run collections on a schedule."""
    logger.info(f"Scheduler started: running every {interval_hours} hour(s)")
    logger.info("Press Ctrl+C to stop\n")

    # Run immediately on start
    run_collection(conn, session, years, house, senate)

    if schedule_lib:
        schedule_lib.every(interval_hours).hours.do(
            run_collection, conn, session, years, house, senate
        )
        while True:
            schedule_lib.run_pending()
            time.sleep(60)
    else:
        # Fallback without schedule library
        while True:
            time.sleep(interval_hours * 3600)
            run_collection(conn, session, years, house, senate)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Collect U.S. Congressional stock trade disclosures",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python congress_trades.py                    # Collect once (current + previous year)
  python congress_trades.py --schedule 6       # Collect every 6 hours
  python congress_trades.py --years 2024 2025 2026
  python congress_trades.py --house-only       # House filings only
  python congress_trades.py --senate-only      # Senate filings only
  python congress_trades.py --query NVDA       # Search trades by ticker
  python congress_trades.py --query-politician Pelosi
  python congress_trades.py --export csv       # Export to CSV
  python congress_trades.py --summary          # Show database summary
        """
    )

    parser.add_argument("--schedule", type=int, metavar="HOURS",
                        help="Run on a schedule (hours between runs)")
    parser.add_argument("--years", type=int, nargs="+",
                        help=f"Years to collect (default: {CURRENT_YEAR-1} {CURRENT_YEAR})")
    parser.add_argument("--house-only", action="store_true",
                        help="Only collect House filings")
    parser.add_argument("--senate-only", action="store_true",
                        help="Only collect Senate filings")
    parser.add_argument("--query", type=str, metavar="TICKER",
                        help="Search trades by ticker symbol")
    parser.add_argument("--query-politician", type=str, metavar="NAME",
                        help="Search trades by politician name")
    parser.add_argument("--query-days", type=int, default=None,
                        help="Limit query to last N days")
    parser.add_argument("--export", type=str, nargs="?", const="csv", metavar="FORMAT",
                        help="Export database to CSV")
    parser.add_argument("--summary", action="store_true",
                        help="Print database summary")
    parser.add_argument("--refresh", action="store_true",
                        help="Delete all existing data and collect fresh")
    parser.add_argument("--purge-before", type=str, metavar="YYYY-MM-DD",
                        help="Delete trades older than this date before collecting")
    parser.add_argument("--db", type=str, default=str(DB_PATH),
                        help=f"Database path (default: {DB_PATH})")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose logging")

    args = parser.parse_args()
    setup_logging(args.verbose)

    # Initialize database
    db_path = Path(args.db)
    conn = init_db(db_path)

    # Handle query/export modes (no collection needed)
    if args.summary:
        print_summary(conn)
        conn.close()
        return

    if args.query or args.query_politician:
        results = query_trades(conn, ticker=args.query,
                               politician=args.query_politician,
                               days=args.query_days)
        print_query_results(results)
        conn.close()
        return

    if args.export:
        path = export_csv(conn)
        print(f"Exported to: {path}")
        conn.close()
        return

    # Data management
    if args.refresh:
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.execute("DELETE FROM trades")
        conn.execute("DELETE FROM collection_log")
        conn.commit()
        conn.execute("VACUUM")
        logger.info(f"Purged all {count} trades. Starting fresh.")
        print(f"Purged {count} trades. Collecting fresh data...")

    if args.purge_before:
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE trade_date < ?",
            (args.purge_before,)
        ).fetchone()[0]
        conn.execute(
            "DELETE FROM trades WHERE trade_date < ?",
            (args.purge_before,)
        )
        conn.commit()
        if count > 0:
            conn.execute("VACUUM")
        logger.info(f"Purged {count} trades before {args.purge_before}")
        print(f"Purged {count} trades before {args.purge_before}")

    # Collection mode
    years = args.years or [CURRENT_YEAR - 1, CURRENT_YEAR]
    house = not args.senate_only
    senate = not args.house_only
    session = make_session()

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logger.info("\nShutting down gracefully...")
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.schedule:
        run_scheduled(args.schedule, conn, session, years, house, senate)
    else:
        run_collection(conn, session, years, house, senate)

    conn.close()


if __name__ == "__main__":
    main()
