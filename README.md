# Congress Trades — Scraper + Scorer

Collects U.S. politician stock trade disclosures from **official government sources**, scores each politician across four dimensions, and surfaces actionable buy signals from top-performing traders. No paid APIs. Runs entirely on your machine.

## Project Structure

```
congress_trades/
├── congress_trades.py       # Data collector — fetches from House Clerk XML + PDFs
├── scorer.py                # Scoring engine — leaderboard + buy signal filter
├── dashboard.py             # Streamlit dashboard — local web UI
├── config.json              # Configurable weights, thresholds, and parameters
├── requirements.txt         # Python dependencies
├── README.md                # This file
├── congress_trades.db       # SQLite database (created on first run)
└── congress_trades.log      # Log file (created on first run)
```

## Quick Start

```bash
pip install -r requirements.txt

# 1. Collect data (both chambers)
python congress_trades.py --years 2025 2026

# Or House only (most reliable)
python congress_trades.py --house-only --years 2025 2026

# Or Senate only (eFD has intermittent availability)
python congress_trades.py --senate-only --years 2025 2026

# Fresh start: purge old data and recollect
python congress_trades.py --refresh --years 2024 2025 2026

# Purge trades older than a date
python congress_trades.py --purge-before 2024-01-01

# 2. Score politicians and view leaderboard
python scorer.py

# 3. Get buy signals from top traders
python scorer.py --signals

# 4. Launch the local dashboard
streamlit run dashboard.py
```

---

## Part 1: Data Collector (`congress_trades.py`)

Downloads and parses official House Clerk financial disclosures into a local SQLite database.

### Data Sources

| Source | URL | Status |
|--------|-----|--------|
| House Clerk XML | `disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.xml` | Primary, fully automated |
| House PTR PDFs | `disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/{DOC_ID}.pdf` | Parsed via `pdfplumber` |
| Senate eFD | `efdsearch.senate.gov/search/report/data/` | DataTables JSON API with CSRF + retry |

### Collector Usage

```bash
# Collect current + previous year (default)
python congress_trades.py

# Specific years
python congress_trades.py --years 2024 2025 2026

# House only (fastest, most reliable)
python congress_trades.py --house-only

# Senate only
python congress_trades.py --senate-only

# Fresh collection (deletes all existing data first)
python congress_trades.py --refresh

# Purge trades before a date, then collect
python congress_trades.py --purge-before 2024-01-01

# Run on a schedule (every 6 hours)
python congress_trades.py --schedule 6

# Search by ticker or politician
python congress_trades.py --query NVDA
python congress_trades.py --query-politician Pelosi

# Export all trades to CSV
python congress_trades.py --export

# Show database summary
python congress_trades.py --summary
```

### How Collection Works

1. Fetches the XML index for each year from the House Clerk
2. Filters for Periodic Transaction Reports (PTRs) — the filings that contain stock trades
3. Downloads each PTR as a PDF and parses the trade table with `pdfplumber`
4. Extracts: politician, trade date, ticker, buy/sell, amount range, asset name, owner
5. Deduplicates via SHA-256 hash and inserts into SQLite

### Senate eFD

The Senate financial disclosures at `efdsearch.senate.gov` use a Django/DataTables backend. The collector:
1. GETs `/search/` to establish a session and obtain the CSRF cookie
2. POSTs to `/search/home/` to accept the terms-of-use agreement
3. POSTs to `/search/report/data/` with DataTables pagination to list PTR filings
4. GETs each individual PTR HTML page and parses the trade table

The server has intermittent availability (503 errors, especially on weekends and evenings). The collector retries with exponential backoff. If it fails, try again later during business hours.

---

## Part 2: Scoring Engine (`scorer.py`)

Scores every politician in the database across four dimensions, produces a ranked leaderboard, and filters for actionable buy signals from top-performing traders.

### Scoring Dimensions

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| Estimated Return | 35% | Size-weighted average return on disclosed buys (live prices via yfinance, or heuristic fallback) |
| Win Rate | 25% | Fraction of buys that went up (price-based or buy/sell pair matching) |
| Position Size | 20% | Average dollar size of buys — larger = higher conviction (log-scaled) |
| Recency | 20% | How recently they traded + trade frequency (exponential decay, 90-day half-life) |

Each dimension scores 0-100, then combined into a weighted composite score.

### Scorer Usage

```bash
# Full leaderboard
python scorer.py

# Top 10 only
python scorer.py --top 10

# Buy signals from politicians scoring >= 60
python scorer.py --signals

# Custom threshold
python scorer.py --signals --threshold 50

# Signals from the last 30 days only
python scorer.py --signals --days 30

# Filter to politicians who trade a specific ticker
python scorer.py --ticker NVDA

# Export leaderboard + signals to CSV
python scorer.py --export

# Skip live price lookups (faster, uses heuristic scoring)
python scorer.py --no-prices

# Use custom config file
python scorer.py --config config.json
```

### Buy Signals

When you run `--signals`, the scorer:
1. Identifies all politicians above the score threshold (default 60)
2. Pulls their recent buy transactions (default last 90 days)
3. Displays them sorted by date with the politician's composite score and rank
4. Detects **clustering** — tickers bought by multiple top traders (strongest signal)

### Output Example

```
  CONGRESSIONAL TRADER LEADERBOARD
  ============================================
  Rank  Politician            St  Score  Return  WinRt  Size  Recncy  Buys  Sells  Tickers
  1     Katherine M. Clark    MA   62.2   65.4  100.0  44.6   26.9     2      1       2
  2     Sheri Biggs           SC   59.9   51.7  100.0  42.0   42.3    25     35      35
  3     Donald S. Beyer Jr    VA   57.9   67.1  100.0  25.4   21.8     3      1       1
  ...

  BUY SIGNALS — Politicians scoring >= 50
  ============================================
  Date        Politician          Score  Ticker  Amount              Asset
  2025-11-18  Gilbert Cisneros     57.5  NVDA    $15,001-$50,000    NVIDIA Corporation
  2025-11-18  Gilbert Cisneros     57.5  MSFT    $50,001-$100,000   Microsoft Corporation
  ...

  CLUSTERING — Tickers bought by multiple top traders:
    GS       (3 traders): Sheri Biggs, Gilbert Cisneros, Rob Bresnahan
    NVDA     (2 traders): Gilbert Cisneros, Rob Bresnahan
```

---

## Configuration (`config.json`)

All scoring parameters are configurable. Edit `config.json` or pass `--config path/to/custom.json`:

```json
{
    "weight_return": 0.35,
    "weight_win_rate": 0.25,
    "weight_size": 0.20,
    "weight_recency": 0.20,
    "signal_threshold": 60,
    "min_trades": 3,
    "lookback_days": 730,
    "hold_period_days": 60,
    "use_live_prices": true,
    "recency_half_life_days": 90
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `weight_return` | 0.35 | Weight for estimated return dimension |
| `weight_win_rate` | 0.25 | Weight for win rate dimension |
| `weight_size` | 0.20 | Weight for position size dimension |
| `weight_recency` | 0.20 | Weight for recency dimension |
| `signal_threshold` | 60 | Minimum composite score for buy signals |
| `min_trades` | 3 | Minimum trades to be scored |
| `lookback_days` | 730 | How far back to look for trades (2 years) |
| `hold_period_days` | 60 | Assumed holding period for return estimation |
| `use_live_prices` | true | Fetch current prices via yfinance (set false for speed) |
| `recency_half_life_days` | 90 | Days until recency score halves |

---

## Database Schema

### `trades` table

| Column | Type | Description |
|--------|------|-------------|
| `politician` | TEXT | Full name |
| `chamber` | TEXT | "House" or "Senate" |
| `state` | TEXT | Two-letter state code |
| `trade_date` | TEXT | ISO date of the trade |
| `filing_date` | TEXT | ISO date the filing was submitted |
| `ticker` | TEXT | Stock ticker symbol |
| `asset_name` | TEXT | Full asset description |
| `trade_type` | TEXT | "buy", "sell", or "exchange" |
| `amount_low` | INTEGER | Lower bound of amount range |
| `amount_high` | INTEGER | Upper bound of amount range |
| `amount_raw` | TEXT | Original amount string |
| `owner` | TEXT | "Self", "Spouse", "Joint", "Dependent Child" |
| `source` | TEXT | "house_clerk" or "senate_efd" |
| `source_url` | TEXT | Direct URL to the source PDF |

### Useful SQL Queries

```sql
-- Recent big buys
SELECT politician, trade_date, ticker, amount_raw
FROM trades
WHERE trade_type = 'buy' AND amount_low >= 100000
ORDER BY trade_date DESC;

-- Tickers bought by multiple politicians this month
SELECT ticker, COUNT(DISTINCT politician) as buyers,
       GROUP_CONCAT(DISTINCT politician) as who
FROM trades
WHERE trade_type = 'buy' AND ticker IS NOT NULL
  AND trade_date >= date('now', '-30 days')
GROUP BY ticker
HAVING buyers >= 2
ORDER BY buyers DESC;

-- Filing delay (late filers may be hiding trades)
SELECT politician, trade_date, filing_date,
       julianday(filing_date) - julianday(trade_date) as delay_days
FROM trades
WHERE trade_date IS NOT NULL AND filing_date IS NOT NULL
ORDER BY delay_days DESC LIMIT 20;
```

---

## Dependencies

| Package | Purpose | Required |
|---------|---------|----------|
| `requests` | HTTP client for XML/PDF fetching | Yes |
| `beautifulsoup4` | HTML parsing (Senate eFD fallback) | Yes |
| `pdfplumber` | PDF table extraction from House PTRs | Yes |
| `schedule` | Recurring collection scheduling | Yes |
| `yfinance` | Live stock prices for accurate return scoring | Optional |

Without `yfinance`, the scorer falls back to heuristic return estimation using buy/sell pair matching.

## Part 3: Dashboard (`dashboard.py`)

Local Streamlit web app that reads from the SQLite database. No external APIs — everything runs off your scraped data.

### Launch

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501`.

### Three Panels

| Tab | What It Shows |
|-----|---------------|
| **Leaderboard** | All scored politicians ranked by composite score, with progress bars and expandable detail cards for top 5 |
| **Buy Signal Feed** | Recent buys from politicians above the score threshold, filterable by ticker, showing amount ranges and asset names |
| **Stocks to Watch** | Tickers bought by multiple politicians within a configurable window (30/60/90/180/365 days), with per-politician breakdowns |

### Sidebar Controls

| Control | Default | What It Does |
|---------|---------|-------------|
| Min Score for Buy Signals | 50 | Only show buys from politicians scoring above this |
| Min Trades to Score | 3 | Minimum trades required for a politician to appear on the leaderboard |
| Stocks to Watch Window | 90 days | Lookback window for the clustering panel |
| Min Buyers for Watch List | 2 | Minimum distinct politicians buying a ticker to flag it |

The dashboard uses the data's own date range (not today's date), so it works correctly even if the database hasn't been updated recently.

## Limitations

- **Amount ranges, not exact figures**: Congress reports in ranges ($1,001-$15,000, etc.)
- **Filing delays**: Trades may appear 10-45+ days after execution
- **Senate coverage**: Senate eFD blocks automation; House data is the reliable source
- **PDF parsing**: Handles standard House PTR format; unusual layouts may miss some trades
- **No party data**: House Clerk XML doesn't include party affiliation

## Rate Limiting

The collector respects government servers: 1.5s delay between House requests, 2.0s for Senate, 3 retries with exponential backoff.
