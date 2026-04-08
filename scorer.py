#!/usr/bin/env python3
"""
Congressional Trader Scoring & Leaderboard
===========================================
Scores each politician in the database across four dimensions, produces
a ranked leaderboard, and filters for actionable buy signals from
top-scoring politicians.

Scoring dimensions (each 0-100, combined into a weighted composite):
  1. Estimated Return   – How well did their disclosed buys perform?
  2. Win Rate           – What % of their buys went up?
  3. Position Sizing    – Do they put real money behind convictions?
  4. Recency            – Are they still actively trading?

Usage:
    python scorer.py                          # Full leaderboard
    python scorer.py --top 10                 # Top 10 only
    python scorer.py --signals                # Buy signals from top traders
    python scorer.py --signals --threshold 60 # Custom score threshold
    python scorer.py --ticker NVDA            # Leaderboard filtered to NVDA traders
    python scorer.py --export                 # Export leaderboard + signals to CSV
    python scorer.py --config config.json     # Load custom weights/thresholds

Requires: congress_trades.db (created by congress_trades.py)
          yfinance (optional, for live price lookups to estimate returns)
"""

import argparse
import csv
import json
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Optional: yfinance for live price lookups
# ---------------------------------------------------------------------------
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress_trades.db"

# ---------------------------------------------------------------------------
# Default configuration — all overridable via config.json
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = {
    # Weights for composite score (must sum to 1.0)
    "weight_return": 0.35,
    "weight_win_rate": 0.25,
    "weight_size": 0.20,
    "weight_recency": 0.20,

    # Thresholds
    "signal_threshold": 60,      # Minimum composite score for buy signals
    "min_trades": 3,             # Minimum trades to be scored
    "lookback_days": 730,        # 2 years of trade history for scoring

    # Return estimation
    "hold_period_days": 60,      # Assumed holding period for return estimation
    "use_live_prices": True,     # Fetch current prices via yfinance (if installed)

    # Recency decay
    "recency_half_life_days": 90, # Days until recency score halves

    # Position sizing tiers (midpoint of each disclosure range)
    "size_midpoints": {
        "1001_15000": 8000,
        "15001_50000": 32500,
        "50001_100000": 75000,
        "100001_250000": 175000,
        "250001_500000": 375000,
        "500001_1000000": 750000,
        "1000001_5000000": 3000000,
        "5000001_25000000": 15000000,
        "25000001_50000000": 37500000,
        "50000001_plus": 75000000,
    },
}


# ---------------------------------------------------------------------------
# Price cache — avoid hammering Yahoo for the same ticker
# ---------------------------------------------------------------------------
_price_cache: dict[str, dict] = {}


def get_price_at_date(ticker: str, date_str: str) -> Optional[float]:
    """Get the closing price of a ticker on a given date.
    Falls back to nearest available trading day."""
    if not HAS_YFINANCE or not ticker or not date_str:
        return None

    cache_key = f"{ticker}_{date_str}"
    if cache_key in _price_cache:
        return _price_cache[cache_key]

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        start = (dt - timedelta(days=5)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=5)).strftime("%Y-%m-%d")

        data = yf.download(ticker, start=start, end=end,
                           progress=False, auto_adjust=True)
        if data is not None and not data.empty:
            # Flatten MultiIndex columns if present
            if hasattr(data.columns, 'levels'):
                data.columns = data.columns.get_level_values(0)

            # Find closest date
            target = dt
            best = None
            best_dist = float("inf")
            for idx in data.index:
                idx_dt = idx.to_pydatetime().replace(tzinfo=None)
                dist = abs((idx_dt - target).days)
                if dist < best_dist:
                    best_dist = dist
                    close_val = data.loc[idx, "Close"]
                    if hasattr(close_val, 'item'):
                        best = close_val.item()
                    else:
                        best = float(close_val)

            _price_cache[cache_key] = best
            return best
    except Exception:
        pass

    _price_cache[cache_key] = None
    return None


def get_current_price(ticker: str) -> Optional[float]:
    """Get the most recent closing price for a ticker."""
    if not HAS_YFINANCE or not ticker:
        return None

    cache_key = f"{ticker}_current"
    if cache_key in _price_cache:
        return _price_cache[cache_key]

    try:
        t = yf.Ticker(ticker)
        info = t.info
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if price:
            _price_cache[cache_key] = float(price)
            return float(price)

        # Fallback: last close from history
        hist = t.history(period="5d")
        if hist is not None and not hist.empty:
            val = hist["Close"].iloc[-1]
            if hasattr(val, 'item'):
                val = val.item()
            _price_cache[cache_key] = float(val)
            return float(val)
    except Exception:
        pass

    _price_cache[cache_key] = None
    return None


# ---------------------------------------------------------------------------
# Core scoring functions
# ---------------------------------------------------------------------------
def estimate_position_midpoint(amount_low: Optional[int],
                               amount_high: Optional[int]) -> float:
    """Estimate the dollar value of a position from its disclosure range."""
    if amount_low and amount_high:
        return (amount_low + amount_high) / 2
    elif amount_low:
        return amount_low * 1.5  # Conservative estimate when no upper bound
    return 8000  # Default to smallest range midpoint


def score_return(trades: list[dict], config: dict) -> tuple[float, dict]:
    """Score a politician's estimated return on buys.

    Strategy:
    - For each buy with a known ticker, estimate return using:
      (a) If yfinance available: actual price change over hold_period_days
      (b) Fallback: use sell transactions of the same ticker as exit signals
    - Score is normalized 0-100 where 0% return = 50, +50% = 100, -50% = 0
    """
    buys = [t for t in trades if t["trade_type"] == "buy" and t["ticker"]]
    if not buys:
        return 0.0, {"estimated_return_pct": None, "scored_buys": 0, "method": "none"}

    hold_days = config.get("hold_period_days", 60)
    use_live = config.get("use_live_prices", True) and HAS_YFINANCE

    returns = []
    for buy in buys:
        ticker = buy["ticker"]
        buy_date = buy["trade_date"]
        if not buy_date:
            continue

        ret = None

        if use_live:
            buy_price = get_price_at_date(ticker, buy_date)
            if buy_price and buy_price > 0:
                # Try price at hold_period_days later
                try:
                    exit_date = datetime.strptime(buy_date, "%Y-%m-%d") + timedelta(days=hold_days)
                    if exit_date > datetime.now():
                        # Position is still open — use current price
                        exit_price = get_current_price(ticker)
                    else:
                        exit_price = get_price_at_date(ticker, exit_date.strftime("%Y-%m-%d"))

                    if exit_price and exit_price > 0:
                        ret = (exit_price - buy_price) / buy_price
                except (ValueError, TypeError):
                    pass

        if ret is None:
            # Fallback: look for a sell of the same ticker after this buy
            sells_after = [
                t for t in trades
                if t["trade_type"] == "sell"
                and t["ticker"] == ticker
                and t["trade_date"]
                and t["trade_date"] > buy_date
            ]
            if sells_after:
                # Can't know exact return without prices, assume positive
                # (they sold = they exited, which at least shows activity)
                ret = 0.05  # Modest assumed return for completed round-trip

        if ret is not None:
            # Weight by position size
            size = estimate_position_midpoint(buy["amount_low"], buy["amount_high"])
            returns.append((ret, size))

    if not returns:
        return 25.0, {"estimated_return_pct": None, "scored_buys": 0, "method": "no_data"}

    # Size-weighted average return
    total_weight = sum(s for _, s in returns)
    if total_weight == 0:
        avg_return = sum(r for r, _ in returns) / len(returns)
    else:
        avg_return = sum(r * s for r, s in returns) / total_weight

    # Normalize to 0-100: -50% → 0, 0% → 50, +50% → 100
    score = max(0, min(100, 50 + (avg_return * 100)))

    method = "live_prices" if use_live else "heuristic"
    return score, {
        "estimated_return_pct": round(avg_return * 100, 2),
        "scored_buys": len(returns),
        "total_buys": len(buys),
        "method": method,
    }


def score_win_rate(trades: list[dict], config: dict) -> tuple[float, dict]:
    """Score the win rate — what fraction of buys had positive returns.

    Without exact prices, we use heuristics:
    - If yfinance available: check if price went up after buy
    - Fallback: check if there's a later sell (implies profitable exit)
             or if the buy was in a generally rising sector period
    """
    buys = [t for t in trades if t["trade_type"] == "buy" and t["ticker"]]
    if not buys:
        return 0.0, {"wins": 0, "losses": 0, "total": 0, "win_rate_pct": None}

    hold_days = config.get("hold_period_days", 60)
    use_live = config.get("use_live_prices", True) and HAS_YFINANCE

    wins = 0
    losses = 0
    scored = 0

    for buy in buys:
        ticker = buy["ticker"]
        buy_date = buy["trade_date"]
        if not buy_date:
            continue

        is_win = None

        if use_live:
            buy_price = get_price_at_date(ticker, buy_date)
            if buy_price and buy_price > 0:
                try:
                    exit_date = datetime.strptime(buy_date, "%Y-%m-%d") + timedelta(days=hold_days)
                    if exit_date > datetime.now():
                        exit_price = get_current_price(ticker)
                    else:
                        exit_price = get_price_at_date(ticker, exit_date.strftime("%Y-%m-%d"))

                    if exit_price and exit_price > 0:
                        is_win = exit_price > buy_price
                except (ValueError, TypeError):
                    pass

        if is_win is None:
            # Fallback: did they sell later at a presumably higher range?
            sells_after = [
                t for t in trades
                if t["trade_type"] == "sell"
                and t["ticker"] == ticker
                and t["trade_date"]
                and t["trade_date"] > buy_date
            ]
            if sells_after:
                sell = sells_after[0]
                sell_mid = estimate_position_midpoint(sell["amount_low"], sell["amount_high"])
                buy_mid = estimate_position_midpoint(buy["amount_low"], buy["amount_high"])
                # If they sold at a higher amount range, likely a win
                is_win = sell_mid >= buy_mid
            # If no sell found, we can't determine — skip

        if is_win is not None:
            scored += 1
            if is_win:
                wins += 1
            else:
                losses += 1

    if scored == 0:
        # Can't determine win rate — give neutral score
        return 50.0, {"wins": 0, "losses": 0, "total": len(buys),
                       "scored": 0, "win_rate_pct": None}

    rate = wins / scored
    score = rate * 100  # 0-100 directly maps to win rate

    return score, {
        "wins": wins,
        "losses": losses,
        "scored": scored,
        "total": len(buys),
        "win_rate_pct": round(rate * 100, 1),
    }


def score_position_size(trades: list[dict], config: dict) -> tuple[float, dict]:
    """Score average position size — larger positions signal higher conviction.

    Uses the midpoint of each disclosure range. Score is logarithmic:
    - $8K (min range) = 10
    - $75K = 30
    - $375K = 50
    - $3M  = 70
    - $15M = 90
    - $75M = 100
    """
    buys = [t for t in trades if t["trade_type"] == "buy"]
    if not buys:
        return 0.0, {"avg_position": None, "total_buys": 0, "total_deployed": None}

    sizes = []
    for b in buys:
        mid = estimate_position_midpoint(b["amount_low"], b["amount_high"])
        sizes.append(mid)

    avg_size = sum(sizes) / len(sizes)
    total_deployed = sum(sizes)

    # Logarithmic scoring: log10(8000) ≈ 3.9, log10(75M) ≈ 7.88
    # Map [3.9, 7.9] → [10, 100]
    log_val = math.log10(max(avg_size, 1000))
    score = max(0, min(100, (log_val - 3.9) / (7.9 - 3.9) * 90 + 10))

    return score, {
        "avg_position": round(avg_size),
        "total_buys": len(buys),
        "total_deployed": round(total_deployed),
        "largest_buy": round(max(sizes)),
    }


def score_recency(trades: list[dict], config: dict) -> tuple[float, dict]:
    """Score how recently the politician has been active.

    Uses exponential decay from the most recent trade date.
    Half-life default: 90 days (score halves every 90 days of inactivity).
    Also factors in trade frequency over the lookback window.
    """
    if not trades:
        return 0.0, {"last_trade": None, "days_since": None, "trades_per_month": 0}

    half_life = config.get("recency_half_life_days", 90)
    now = datetime.now()

    # Find most recent trade
    dates = []
    for t in trades:
        if t["trade_date"]:
            try:
                dates.append(datetime.strptime(t["trade_date"], "%Y-%m-%d"))
            except ValueError:
                pass

    if not dates:
        return 0.0, {"last_trade": None, "days_since": None, "trades_per_month": 0}

    latest = max(dates)
    earliest = min(dates)
    days_since = (now - latest).days
    span_months = max(1, (latest - earliest).days / 30)
    trades_per_month = len(dates) / span_months

    # Decay component (70% of recency score)
    decay = math.exp(-0.693 * days_since / half_life)  # 0.693 = ln(2)
    decay_score = decay * 100

    # Frequency bonus (30% of recency score)
    # 1 trade/month = 30, 5/month = 60, 10+/month = 100
    freq_score = min(100, trades_per_month * 10)

    score = decay_score * 0.7 + freq_score * 0.3

    return min(100, score), {
        "last_trade": latest.strftime("%Y-%m-%d"),
        "days_since": days_since,
        "trades_per_month": round(trades_per_month, 1),
        "total_trades": len(dates),
    }


def compute_composite_score(return_score: float, win_score: float,
                            size_score: float, recency_score: float,
                            config: dict) -> float:
    """Compute weighted composite score."""
    w_ret = config.get("weight_return", 0.35)
    w_win = config.get("weight_win_rate", 0.25)
    w_size = config.get("weight_size", 0.20)
    w_rec = config.get("weight_recency", 0.20)

    composite = (
        return_score * w_ret
        + win_score * w_win
        + size_score * w_size
        + recency_score * w_rec
    )
    return round(composite, 1)


# ---------------------------------------------------------------------------
# Main scoring pipeline
# ---------------------------------------------------------------------------
def load_trades(conn: sqlite3.Connection,
                lookback_days: int = 730) -> dict[str, list[dict]]:
    """Load trades from the database grouped by politician."""
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT politician, chamber, party, state, district,
               trade_date, filing_date, ticker, asset_name,
               trade_type, amount_low, amount_high, amount_raw,
               owner, doc_id, source
        FROM trades
        WHERE trade_date >= ? OR trade_date IS NULL
        ORDER BY politician, trade_date
    """, (cutoff,)).fetchall()

    columns = [
        "politician", "chamber", "party", "state", "district",
        "trade_date", "filing_date", "ticker", "asset_name",
        "trade_type", "amount_low", "amount_high", "amount_raw",
        "owner", "doc_id", "source",
    ]

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        d = dict(zip(columns, row))
        grouped[d["politician"]].append(d)

    return dict(grouped)


def score_all_politicians(conn: sqlite3.Connection,
                          config: dict = None) -> list[dict]:
    """Score every politician and return sorted leaderboard."""
    if config is None:
        config = DEFAULT_CONFIG.copy()

    min_trades = config.get("min_trades", 3)
    lookback = config.get("lookback_days", 730)

    grouped = load_trades(conn, lookback)
    results = []

    total = len(grouped)
    print(f"\n  Scoring {total} politicians...", flush=True)

    for i, (politician, trades) in enumerate(grouped.items(), 1):
        if len(trades) < min_trades:
            continue

        # Compute each dimension
        ret_score, ret_detail = score_return(trades, config)
        win_score, win_detail = score_win_rate(trades, config)
        size_score, size_detail = score_position_size(trades, config)
        rec_score, rec_detail = score_recency(trades, config)

        composite = compute_composite_score(ret_score, win_score,
                                            size_score, rec_score, config)

        # Metadata
        buys = [t for t in trades if t["trade_type"] == "buy"]
        sells = [t for t in trades if t["trade_type"] == "sell"]
        tickers = set(t["ticker"] for t in trades if t["ticker"])
        chamber = trades[0].get("chamber", "")
        state = trades[0].get("state", "")

        results.append({
            "politician": politician,
            "chamber": chamber,
            "state": state,
            "composite_score": composite,
            "return_score": round(ret_score, 1),
            "win_rate_score": round(win_score, 1),
            "size_score": round(size_score, 1),
            "recency_score": round(rec_score, 1),
            "total_trades": len(trades),
            "total_buys": len(buys),
            "total_sells": len(sells),
            "unique_tickers": len(tickers),
            "return_detail": ret_detail,
            "win_detail": win_detail,
            "size_detail": size_detail,
            "recency_detail": rec_detail,
        })

        # Progress indicator
        if i % 10 == 0 or i == total:
            print(f"    [{i}/{total}]", flush=True)

    # Sort by composite score descending
    results.sort(key=lambda x: x["composite_score"], reverse=True)

    # Add rank
    for i, r in enumerate(results, 1):
        r["rank"] = i

    return results


# ---------------------------------------------------------------------------
# Buy signal filter
# ---------------------------------------------------------------------------
def get_buy_signals(conn: sqlite3.Connection, leaderboard: list[dict],
                    config: dict = None, days: int = 90,
                    ticker: str = None) -> list[dict]:
    """Get recent buy transactions from politicians above the score threshold."""
    if config is None:
        config = DEFAULT_CONFIG.copy()

    threshold = config.get("signal_threshold", 60)

    # Politicians above threshold
    qualified = {r["politician"]: r for r in leaderboard
                 if r["composite_score"] >= threshold}

    if not qualified:
        return []

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = """
        SELECT politician, chamber, state, trade_date, filing_date,
               ticker, asset_name, trade_type, amount_low, amount_high,
               amount_raw, owner, doc_id, source_url
        FROM trades
        WHERE trade_type = 'buy'
          AND trade_date >= ?
          AND ticker IS NOT NULL
    """
    params: list = [cutoff]

    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())

    query += " ORDER BY trade_date DESC"

    rows = conn.execute(query, params).fetchall()
    columns = [
        "politician", "chamber", "state", "trade_date", "filing_date",
        "ticker", "asset_name", "trade_type", "amount_low", "amount_high",
        "amount_raw", "owner", "doc_id", "source_url",
    ]

    signals = []
    for row in rows:
        d = dict(zip(columns, row))
        pol = d["politician"]
        if pol in qualified:
            d["composite_score"] = qualified[pol]["composite_score"]
            d["return_score"] = qualified[pol]["return_score"]
            d["win_rate_score"] = qualified[pol]["win_rate_score"]
            d["rank"] = qualified[pol]["rank"]
            signals.append(d)

    return signals


# ---------------------------------------------------------------------------
# Display functions
# ---------------------------------------------------------------------------
def print_leaderboard(results: list[dict], top_n: int = None, ticker: str = None):
    """Pretty-print the leaderboard.
    
    Note: ticker filtering is handled by the caller (main) before this
    function is invoked, so we just display whatever results are passed in.
    """
    display = results[:top_n] if top_n else results

    if not display:
        print("\n  No politicians meet the minimum trade threshold.\n")
        return

    # Header
    print(f"\n  {'='*110}")
    print(f"  CONGRESSIONAL TRADER LEADERBOARD")
    print(f"  {'='*110}")
    print(f"  {'Rank':<5} {'Politician':<28} {'St':<4} {'Score':>6} "
          f"{'Return':>7} {'WinRt':>6} {'Size':>6} {'Recncy':>7} "
          f"{'Buys':>5} {'Sells':>5} {'Tickers':>8}")
    print(f"  {'-'*5} {'-'*28} {'-'*4} {'-'*6} "
          f"{'-'*7} {'-'*6} {'-'*6} {'-'*7} "
          f"{'-'*5} {'-'*5} {'-'*8}")

    for r in display:
        print(f"  {r['rank']:<5} {r['politician'][:27]:<28} {r['state'] or '':>2}  "
              f"{r['composite_score']:>5.1f} "
              f"{r['return_score']:>6.1f} {r['win_rate_score']:>5.1f} "
              f"{r['size_score']:>5.1f} {r['recency_score']:>6.1f} "
              f"{r['total_buys']:>5} {r['total_sells']:>5} "
              f"{r['unique_tickers']:>8}")

    print(f"  {'='*110}")
    print(f"  {len(display)} politician(s) displayed "
          f"({len(results)} total scored)\n")

    # Show scoring details for top 5
    for r in display[:5]:
        ret_d = r.get("return_detail", {})
        win_d = r.get("win_detail", {})
        size_d = r.get("size_detail", {})
        rec_d = r.get("recency_detail", {})

        est_ret = ret_d.get("estimated_return_pct")
        est_ret_str = f"{est_ret:+.1f}%" if est_ret is not None else "N/A"
        win_pct = win_d.get("win_rate_pct")
        win_str = f"{win_pct:.0f}%" if win_pct is not None else "N/A"
        avg_pos = size_d.get("avg_position")
        avg_pos_str = f"${avg_pos:,.0f}" if avg_pos else "N/A"
        deployed = size_d.get("total_deployed")
        deployed_str = f"${deployed:,.0f}" if deployed else "N/A"
        last_trade = rec_d.get("last_trade", "N/A")
        tpm = rec_d.get("trades_per_month", 0)

        print(f"  #{r['rank']} {r['politician']}")
        print(f"     Est. Return: {est_ret_str}  |  Win Rate: {win_str} "
              f"({win_d.get('wins', 0)}W/{win_d.get('losses', 0)}L of "
              f"{win_d.get('scored', 0)} scored)")
        print(f"     Avg Position: {avg_pos_str}  |  Total Deployed: {deployed_str}")
        print(f"     Last Trade: {last_trade}  |  Frequency: "
              f"{tpm:.1f} trades/month  |  Method: {ret_d.get('method', 'N/A')}")
        print()


def print_signals(signals: list[dict], threshold: float):
    """Pretty-print buy signals."""
    if not signals:
        print(f"\n  No buy signals from politicians scoring >= {threshold}.\n")
        return

    print(f"\n  {'='*120}")
    print(f"  BUY SIGNALS — Politicians scoring >= {threshold}")
    print(f"  {'='*120}")
    print(f"  {'Date':<12} {'Politician':<25} {'Score':>6} {'Rank':>5} "
          f"{'Ticker':<8} {'Amount':<25} {'Asset':<35}")
    print(f"  {'-'*12} {'-'*25} {'-'*6} {'-'*5} "
          f"{'-'*8} {'-'*25} {'-'*35}")

    for s in signals:
        date = (s.get("trade_date") or "")[:10]
        name = s["politician"][:24]
        score = s.get("composite_score", 0)
        rank = s.get("rank", "-")
        ticker = (s.get("ticker") or "N/A")[:7]
        amount = (s.get("amount_raw") or "N/A")[:24]
        asset = (s.get("asset_name") or "")[:34]

        print(f"  {date:<12} {name:<25} {score:>5.1f} {rank:>5} "
              f"{ticker:<8} {amount:<25} {asset:<35}")

    print(f"  {'='*120}")
    print(f"  {len(signals)} buy signal(s)\n")

    # Summary: cluster analysis
    ticker_counts = defaultdict(list)
    for s in signals:
        if s.get("ticker"):
            ticker_counts[s["ticker"]].append(s["politician"])

    multi = {t: p for t, p in ticker_counts.items() if len(set(p)) >= 2}
    if multi:
        print(f"  CLUSTERING — Tickers bought by multiple top traders:")
        for ticker, pols in sorted(multi.items(), key=lambda x: -len(set(x[1]))):
            unique_pols = list(dict.fromkeys(pols))  # preserve order, dedupe
            print(f"    {ticker:<8} ({len(unique_pols)} traders): "
                  f"{', '.join(unique_pols[:5])}")
        print()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def export_leaderboard(results: list[dict], signals: list[dict],
                       output_dir: Path = BASE_DIR):
    """Export leaderboard and signals to CSV files."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Leaderboard CSV
    lb_path = output_dir / f"leaderboard_{ts}.csv"
    lb_columns = [
        "rank", "politician", "chamber", "state", "composite_score",
        "return_score", "win_rate_score", "size_score", "recency_score",
        "total_trades", "total_buys", "total_sells", "unique_tickers",
    ]
    with open(lb_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=lb_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    print(f"  Leaderboard exported to: {lb_path}")

    # Signals CSV
    if signals:
        sig_path = output_dir / f"buy_signals_{ts}.csv"
        sig_columns = [
            "trade_date", "politician", "composite_score", "rank",
            "ticker", "asset_name", "amount_raw", "amount_low", "amount_high",
            "owner", "source_url",
        ]
        with open(sig_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sig_columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(signals)
        print(f"  Buy signals exported to: {sig_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Score congressional traders and generate buy signals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scorer.py                            # Full leaderboard
  python scorer.py --top 10                   # Top 10 only
  python scorer.py --signals                  # Buy signals from top traders
  python scorer.py --signals --threshold 50   # Lower threshold
  python scorer.py --signals --days 30        # Signals from last 30 days
  python scorer.py --ticker NVDA              # Filter to NVDA traders
  python scorer.py --export                   # Export to CSV
  python scorer.py --config config.json       # Custom weights
        """
    )

    parser.add_argument("--top", type=int, metavar="N",
                        help="Show top N politicians only")
    parser.add_argument("--signals", action="store_true",
                        help="Show buy signals from top-scoring politicians")
    parser.add_argument("--threshold", type=float, default=None,
                        help="Minimum composite score for signals (default: 60)")
    parser.add_argument("--days", type=int, default=90,
                        help="Lookback days for buy signals (default: 90)")
    parser.add_argument("--ticker", type=str, metavar="SYMBOL",
                        help="Filter to politicians who traded this ticker")
    parser.add_argument("--export", action="store_true",
                        help="Export leaderboard and signals to CSV")
    parser.add_argument("--config", type=str, metavar="PATH",
                        help="Path to config JSON file")
    parser.add_argument("--db", type=str, default=str(DB_PATH),
                        help=f"Database path (default: {DB_PATH})")
    parser.add_argument("--no-prices", action="store_true",
                        help="Skip live price lookups (faster, less accurate)")

    args = parser.parse_args()

    # Load config
    config = DEFAULT_CONFIG.copy()
    if args.config:
        try:
            with open(args.config) as f:
                user_config = json.load(f)
            config.update(user_config)
            print(f"  Loaded config from {args.config}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"  Warning: Could not load config: {e}")

    if args.threshold is not None:
        config["signal_threshold"] = args.threshold
    if args.no_prices:
        config["use_live_prices"] = False

    # Open database
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"  Error: Database not found at {db_path}")
        print(f"  Run congress_trades.py first to collect trade data.")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))

    # Quick stats
    total_trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    total_pols = conn.execute("SELECT COUNT(DISTINCT politician) FROM trades").fetchone()[0]
    print(f"\n  Database: {total_trades:,} trades from {total_pols} politicians")

    # Score all politicians
    leaderboard = score_all_politicians(conn, config)

    # Filter by ticker if requested
    if args.ticker:
        ticker_pols = set(
            row[0] for row in conn.execute(
                "SELECT DISTINCT politician FROM trades WHERE ticker = ?",
                (args.ticker.upper(),)
            ).fetchall()
        )
        leaderboard = [r for r in leaderboard if r["politician"] in ticker_pols]
        # Re-rank
        for i, r in enumerate(leaderboard, 1):
            r["rank"] = i

    # Display leaderboard
    print_leaderboard(leaderboard, top_n=args.top, ticker=args.ticker)

    # Show signals if requested
    signals = []
    if args.signals:
        signals = get_buy_signals(conn, leaderboard, config,
                                   days=args.days, ticker=args.ticker)
        print_signals(signals, config["signal_threshold"])

    # Export if requested
    if args.export:
        if not args.signals:
            signals = get_buy_signals(conn, leaderboard, config, days=args.days)
        export_leaderboard(leaderboard, signals)

    conn.close()


if __name__ == "__main__":
    main()
