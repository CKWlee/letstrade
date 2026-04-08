#!/usr/bin/env python3
"""
Congress Trades — Dashboard Backend
Run: python app.py
"""

import math, sqlite3, json, traceback
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, jsonify, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress_trades.db"

app = Flask(__name__, static_folder="static")

# ---------------------------------------------------------------------------
# Price cache (avoid re-fetching during same session)
# ---------------------------------------------------------------------------
_price_cache = {}
_price_cache_time = None

# Tickers known to fail or be delisted — skip them to save time
_BAD_TICKERS = set()

def get_prices(tickers):
    """Batch-fetch current prices via yfinance. Returns {ticker: price}."""
    global _price_cache, _price_cache_time
    now = datetime.now()
    if _price_cache_time and (now - _price_cache_time).seconds < 300:
        missing = [t for t in tickers if t not in _price_cache and t not in _BAD_TICKERS]
        if not missing:
            return _price_cache

    try:
        import yfinance as yf
        # Filter: alpha only, 1-6 chars, not previously failed
        clean = [t for t in tickers if t and 1 <= len(t) <= 6
                 and t.isalpha() and t not in _BAD_TICKERS]
        if not clean:
            return _price_cache

        # Batch download with short timeout
        data = yf.download(clean, period="5d", progress=False, timeout=8)
        if data.empty:
            return _price_cache
        for t in clean:
            try:
                if len(clean) == 1:
                    p = float(data["Close"].dropna().iloc[-1])
                else:
                    p = float(data["Close"][t].dropna().iloc[-1])
                if not math.isnan(p) and p > 0:
                    _price_cache[t] = round(p, 2)
                else:
                    _BAD_TICKERS.add(t)
            except Exception:
                _BAD_TICKERS.add(t)
        _price_cache_time = now
    except Exception as e:
        print(f"Price fetch error: {e}")
    return _price_cache


def get_historical_price(ticker, date_str):
    """Get closing price on or near a specific date."""
    try:
        import yfinance as yf
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        start = (dt - timedelta(days=5)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=3)).strftime("%Y-%m-%d")
        data = yf.download(ticker, start=start, end=end, progress=False)
        if data.empty:
            return None
        # Find closest date
        idx = data.index.get_indexer([dt], method="nearest")[0]
        return round(float(data["Close"].iloc[idx]), 2)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def midpoint(low, high):
    if low and high: return (low + high) / 2
    if low: return low * 1.5
    return 8000

def fmt_amount(low, high):
    ranges = [(1001,15000,"$1K–$15K"),(15001,50000,"$15K–$50K"),
              (50001,100000,"$50K–$100K"),(100001,250000,"$100K–$250K"),
              (250001,500000,"$250K–$500K"),(500001,1000000,"$500K–$1M"),
              (1000001,5000000,"$1M–$5M"),(5000001,25000000,"$5M–$25M")]
    if low:
        for lo,hi,label in ranges:
            if low == lo: return label
    if low and high: return f"${low:,.0f}–${high:,.0f}"
    if low: return f"${low:,.0f}+"
    return "N/A"

# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def score_all():
    conn = get_db()
    rows = conn.execute("""
        SELECT politician, chamber, state, trade_date, ticker, asset_name,
               trade_type, amount_low, amount_high, amount_raw, owner, source_url
        FROM trades WHERE trade_date IS NOT NULL ORDER BY trade_date DESC
    """).fetchall()
    conn.close()
    if not rows: return [], [], {}

    trades_by_pol = {}
    all_trades = []
    for r in rows:
        d = dict(r)
        all_trades.append(d)
        pol = d["politician"]
        if pol not in trades_by_pol: trades_by_pol[pol] = []
        trades_by_pol[pol].append(d)

    dates = [r["trade_date"] for r in rows if r["trade_date"]]
    latest_str = max(dates)
    latest_dt = datetime.strptime(latest_str, "%Y-%m-%d")

    results = []
    for pol, trades in trades_by_pol.items():
        if len(trades) < 3: continue
        buys = [t for t in trades if t["trade_type"] == "buy"]
        sells = [t for t in trades if t["trade_type"] == "sell"]
        all_tickers = set(t["ticker"] for t in trades if t["ticker"])

        # Return heuristic
        returns = []
        for b in buys:
            t = b["ticker"]
            if not t or not b["trade_date"]: continue
            later = [s for s in sells if s["ticker"]==t and s["trade_date"] and s["trade_date"]>b["trade_date"]]
            if later:
                sm, bm = midpoint(later[0]["amount_low"],later[0]["amount_high"]), midpoint(b["amount_low"],b["amount_high"])
                ret = max(-0.5, min(1.0, (sm-bm)/bm if bm>0 else 0.05))
                returns.append((ret, bm))
            else:
                returns.append((0.05, midpoint(b["amount_low"],b["amount_high"])))

        if returns:
            tw = sum(s for _,s in returns)
            avg_ret = sum(r*s for r,s in returns)/tw if tw>0 else 0
            return_score = max(0, min(100, 50+avg_ret*100))
            est_return_pct = round(avg_ret*100, 1)
        else:
            return_score, est_return_pct = 25, 0

        wins, scored = 0, 0
        for b in buys:
            t = b["ticker"]
            if not t or not b["trade_date"]: continue
            later = [s for s in sells if s["ticker"]==t and s["trade_date"] and s["trade_date"]>b["trade_date"]]
            if later:
                scored += 1
                if midpoint(later[0]["amount_low"],later[0]["amount_high"]) >= midpoint(b["amount_low"],b["amount_high"]):
                    wins += 1
        win_rate = wins/scored if scored>0 else None
        win_score = (win_rate*100) if win_rate is not None else 50

        sizes = [midpoint(b["amount_low"],b["amount_high"]) for b in buys]
        avg_size = sum(sizes)/len(sizes) if sizes else 8000
        total_dep = sum(sizes)
        size_score = max(0, min(100, (math.log10(max(avg_size,1000))-3.9)/(7.9-3.9)*90+10))

        tdates = [datetime.strptime(t["trade_date"],"%Y-%m-%d") for t in trades if t["trade_date"]]
        if tdates:
            lt = max(tdates)
            days_since = (latest_dt-lt).days
            span = max(1,(lt-min(tdates)).days/30)
            tpm = len(tdates)/span
            decay = math.exp(-0.693*days_since/90)
            recency_score = min(100, decay*100*0.7+min(100,tpm*10)*0.3)
        else:
            recency_score, tpm, lt = 0, 0, None

        composite = return_score*0.35 + win_score*0.25 + size_score*0.20 + recency_score*0.20

        results.append({
            "politician": pol, "state": trades[0].get("state","") or "",
            "chamber": trades[0].get("chamber","") or "",
            "score": round(composite,1), "return_score": round(return_score,1),
            "win_score": round(win_score,1), "size_score": round(size_score,1),
            "recency_score": round(recency_score,1), "est_return_pct": est_return_pct,
            "win_rate_pct": round(win_rate*100) if win_rate is not None else None,
            "avg_position": round(avg_size), "total_deployed": round(total_dep),
            "buys": len(buys), "sells": len(sells), "unique_tickers": len(all_tickers),
            "trades_per_month": round(tpm,1),
            "last_trade": lt.strftime("%Y-%m-%d") if lt else None,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    for i,r in enumerate(results): r["rank"] = i+1
    return results, all_trades, {"latest": latest_str, "earliest": min(dates)}


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
@app.route("/api/briefing")
def briefing():
    leaderboard, all_trades, date_range = score_all()
    if not leaderboard:
        return jsonify({"error": "No data. Run the scraper first."})

    score_map = {r["politician"]: r for r in leaderboard}
    latest_dt = datetime.strptime(date_range["latest"], "%Y-%m-%d")
    now = datetime.now()
    days_stale = (now - latest_dt).days

    cutoff_90 = (latest_dt - timedelta(days=90)).strftime("%Y-%m-%d")

    # Collect all signal tickers for batch price fetch
    recent_buys = [t for t in all_trades if t["trade_type"]=="buy" and t["ticker"] and t["trade_date"] and t["trade_date"]>=cutoff_90]
    recent_sells = [t for t in all_trades if t["trade_type"]=="sell" and t["ticker"] and t["trade_date"] and t["trade_date"]>=cutoff_90]
    all_signal_tickers = list(set(t["ticker"] for t in recent_buys+recent_sells if t["ticker"]))

    # Batch fetch prices
    prices = get_prices(all_signal_tickers)

    # Build buy signals
    buy_ticker_data = {}
    for t in recent_buys:
        tk = t["ticker"]
        if tk not in buy_ticker_data:
            buy_ticker_data[tk] = {"buys":[], "politicians":set(), "asset_name":t["asset_name"]}
        buy_ticker_data[tk]["buys"].append(t)
        buy_ticker_data[tk]["politicians"].add(t["politician"])

    buy_signals = []
    for tk, data in buy_ticker_data.items():
        n_buyers = len(data["politicians"])
        n_buys = len(data["buys"])
        last_buy = max(b["trade_date"] for b in data["buys"])
        days_ago = (latest_dt - datetime.strptime(last_buy,"%Y-%m-%d")).days
        recency = math.exp(-0.693*days_ago/45)
        avg_pol_score = sum(score_map[p]["score"] for p in data["politicians"] if p in score_map)/max(1,sum(1 for p in data["politicians"] if p in score_map))
        total_cap = sum(midpoint(b["amount_low"],b["amount_high"]) for b in data["buys"])
        weight = (n_buyers**1.5)*math.log(n_buys+1)*recency*(avg_pol_score/50)

        pols = []
        for p in sorted(data["politicians"]):
            ps = score_map.get(p,{})
            pb = [b for b in data["buys"] if b["politician"]==p]
            pols.append({"name":p,"score":ps.get("score",0),"state":ps.get("state",""),
                         "buys":len(pb),"last_buy":max(b["trade_date"] for b in pb),
                         "amount":fmt_amount(pb[0]["amount_low"],pb[0]["amount_high"])})

        buy_signals.append({
            "ticker":tk, "asset_name":data["asset_name"] or tk,
            "signal_strength":round(weight,2), "buyers":n_buyers, "total_buys":n_buys,
            "total_capital":round(total_cap), "avg_politician_score":round(avg_pol_score,1),
            "last_buy":last_buy, "days_ago":days_ago, "politicians":pols,
            "price": prices.get(tk),
        })
    buy_signals.sort(key=lambda x: x["signal_strength"], reverse=True)

    # Build sell signals
    sell_ticker_data = {}
    for t in recent_sells:
        tk = t["ticker"]
        if tk not in sell_ticker_data:
            sell_ticker_data[tk] = {"sells":[], "politicians":set(), "asset_name":t["asset_name"]}
        sell_ticker_data[tk]["sells"].append(t)
        sell_ticker_data[tk]["politicians"].add(t["politician"])

    sell_signals = []
    for tk, data in sell_ticker_data.items():
        sell_signals.append({
            "ticker":tk, "asset_name":data["asset_name"] or tk,
            "sellers":len(data["politicians"]), "total_sells":len(data["sells"]),
            "total_capital":round(sum(midpoint(s["amount_low"],s["amount_high"]) for s in data["sells"])),
            "last_sell":max(s["trade_date"] for s in data["sells"]),
            "days_ago":(latest_dt-datetime.strptime(max(s["trade_date"] for s in data["sells"]),"%Y-%m-%d")).days,
            "politicians":sorted(data["politicians"]),
            "price": prices.get(tk),
        })
    sell_signals.sort(key=lambda x: (x["sellers"],x["total_sells"]), reverse=True)

    # Net signals — cross-reference buys and sells per ticker
    all_tickers_in_play = set(s["ticker"] for s in buy_signals) | set(s["ticker"] for s in sell_signals)
    buy_map = {s["ticker"]:s for s in buy_signals}
    sell_map = {s["ticker"]:s for s in sell_signals}

    net_signals = []
    for tk in all_tickers_in_play:
        b = buy_map.get(tk)
        s = sell_map.get(tk)
        n_buyers = b["buyers"] if b else 0
        n_sellers = s["sellers"] if s else 0
        buy_cap = b["total_capital"] if b else 0
        sell_cap = s["total_capital"] if s else 0
        net_cap = buy_cap - sell_cap

        if n_buyers > 0 and n_sellers > 0:
            direction = "mixed"
        elif n_buyers > 0:
            direction = "bullish"
        else:
            direction = "bearish"

        net_signals.append({
            "ticker": tk,
            "direction": direction,
            "buyers": n_buyers,
            "sellers": n_sellers,
            "buy_capital": buy_cap,
            "sell_capital": sell_cap,
            "net_capital": net_cap,
            "price": prices.get(tk),
        })
    net_signals.sort(key=lambda x: abs(x["net_capital"]), reverse=True)

    # Big moves
    cutoff_30 = (latest_dt - timedelta(days=30)).strftime("%Y-%m-%d")
    big_moves = sorted(
        [t for t in all_trades if t["trade_date"] and t["trade_date"]>=cutoff_30 and t["amount_low"] and t["amount_low"]>=50000],
        key=lambda x: x["amount_low"] or 0, reverse=True
    )[:12]
    big_moves_out = []
    for t in big_moves:
        ps = score_map.get(t["politician"],{})
        big_moves_out.append({
            "date":t["trade_date"],"politician":t["politician"],"score":ps.get("score",0),
            "ticker":t["ticker"],"type":t["trade_type"],
            "amount":fmt_amount(t["amount_low"],t["amount_high"]),"asset":t["asset_name"],
            "price": prices.get(t["ticker"]) if t["ticker"] else None,
        })

    # Overview
    total_buys = sum(1 for t in all_trades if t["trade_type"]=="buy")
    total_sells = sum(1 for t in all_trades if t["trade_type"]=="sell")

    # Strongest signal for "Right Now" tile
    top_signal = buy_signals[0] if buy_signals else None
    newest_trade_date = date_range["latest"]

    return jsonify({
        "generated_at": now.strftime("%Y-%m-%d %H:%M"),
        "data_range": date_range,
        "days_stale": days_stale,
        "newest_trade": newest_trade_date,
        "overview": {
            "total_buys": total_buys, "total_sells": total_sells,
            "politicians": len(set(t["politician"] for t in all_trades)),
            "total_trades": len(all_trades),
        },
        "top_signal": {
            "ticker": top_signal["ticker"],
            "buyers": top_signal["buyers"],
            "capital": top_signal["total_capital"],
            "price": top_signal.get("price"),
            "signal_strength": top_signal["signal_strength"],
        } if top_signal else None,
        "leaderboard": leaderboard[:10],
        "buy_signals": buy_signals[:20],
        "sell_signals": sell_signals[:15],
        "net_signals": net_signals[:15],
        "convergence": [s for s in buy_signals if s["buyers"]>=2][:10],
        "big_moves": big_moves_out,
    })

# ---------------------------------------------------------------------------
# Personal Stock Recommendation API
# ---------------------------------------------------------------------------
@app.route("/api/picks")
def picks():
    """Returns top N stock picks to buy or sell right now.
    
    Query params:
      ?action=buy (default) or sell or both
      ?n=5 (how many picks, default 5)
      ?min_score=50 (min politician score to consider, default 0)
      ?min_buyers=1 (min distinct buyers for buy picks, default 1)
    
    Example:
      /api/picks?action=buy&n=5
      /api/picks?action=sell&n=3
      /api/picks?action=both&n=5&min_score=60
    """
    from flask import request
    action = request.args.get("action", "buy")
    n = min(int(request.args.get("n", 5)), 50)
    min_score = float(request.args.get("min_score", 0))
    min_buyers = int(request.args.get("min_buyers", 1))

    leaderboard, all_trades, date_range = score_all()
    if not leaderboard:
        return jsonify({"error": "No data."})

    score_map = {r["politician"]: r for r in leaderboard}
    latest_dt = datetime.strptime(date_range["latest"], "%Y-%m-%d")
    cutoff = (latest_dt - timedelta(days=90)).strftime("%Y-%m-%d")

    recent = [t for t in all_trades if t["ticker"] and t["trade_date"] and t["trade_date"] >= cutoff]
    all_tickers = list(set(t["ticker"] for t in recent))
    prices = get_prices(all_tickers)

    result = {"generated_at": datetime.now().isoformat(), "action": action, "picks": []}

    if action in ("buy", "both"):
        buy_data = {}
        for t in recent:
            if t["trade_type"] != "buy": continue
            tk = t["ticker"]
            if tk not in buy_data: buy_data[tk] = {"buys":[], "pols":set(), "asset": t["asset_name"]}
            buy_data[tk]["buys"].append(t)
            buy_data[tk]["pols"].add(t["politician"])

        buy_picks = []
        for tk, d in buy_data.items():
            nb = len(d["pols"])
            if nb < min_buyers: continue
            avg_sc = sum(score_map[p]["score"] for p in d["pols"] if p in score_map) / max(1, sum(1 for p in d["pols"] if p in score_map))
            if avg_sc < min_score: continue
            last = max(b["trade_date"] for b in d["buys"])
            days_ago = (latest_dt - datetime.strptime(last, "%Y-%m-%d")).days
            recency = math.exp(-0.693 * days_ago / 45)
            cap = sum(midpoint(b["amount_low"], b["amount_high"]) for b in d["buys"])
            weight = (nb ** 1.5) * math.log(len(d["buys"]) + 1) * recency * (avg_sc / 50)

            # Conviction tier
            if nb >= 2 and cap >= 50000 and avg_sc >= 60:
                tier = 1
            elif nb >= 2 or (cap >= 50000 and avg_sc >= 50):
                tier = 2
            else:
                tier = 3

            buy_picks.append({
                "ticker": tk,
                "action": "BUY",
                "price": prices.get(tk),
                "signal_strength": round(weight, 2),
                "tier": tier,
                "buyers": nb,
                "total_buys": len(d["buys"]),
                "capital_deployed": round(cap),
                "avg_politician_score": round(avg_sc, 1),
                "days_since_last_buy": days_ago,
                "politicians": [p for p in sorted(d["pols"])],
                "asset_name": d["asset"] or tk,
                "convergence": nb >= 2,
            })
        buy_picks.sort(key=lambda x: x["signal_strength"], reverse=True)
        result["picks"].extend(buy_picks[:n])

    if action in ("sell", "both"):
        sell_data = {}
        for t in recent:
            if t["trade_type"] != "sell": continue
            tk = t["ticker"]
            if tk not in sell_data: sell_data[tk] = {"sells":[], "pols":set(), "asset": t["asset_name"]}
            sell_data[tk]["sells"].append(t)
            sell_data[tk]["pols"].add(t["politician"])

        sell_picks = []
        for tk, d in sell_data.items():
            ns = len(d["pols"])
            cap = sum(midpoint(s["amount_low"], s["amount_high"]) for s in d["sells"])
            last = max(s["trade_date"] for s in d["sells"])
            days_ago = (latest_dt - datetime.strptime(last, "%Y-%m-%d")).days
            sell_picks.append({
                "ticker": tk,
                "action": "SELL",
                "price": prices.get(tk),
                "sellers": ns,
                "total_sells": len(d["sells"]),
                "capital_exited": round(cap),
                "days_since_last_sell": days_ago,
                "politicians": sorted(d["pols"]),
                "asset_name": d["asset"] or tk,
            })
        sell_picks.sort(key=lambda x: (x["sellers"], x["total_sells"]), reverse=True)
        result["picks"].extend(sell_picks[:n])

    return jsonify(result)


@app.route("/api/ticker/<ticker>")
def ticker_detail(ticker):
    """Get full detail on a specific ticker — who's buying, who's selling, net direction, price."""
    ticker = ticker.upper()
    leaderboard, all_trades, date_range = score_all()
    if not leaderboard:
        return jsonify({"error": "No data."})

    score_map = {r["politician"]: r for r in leaderboard}
    prices = get_prices([ticker])

    trades = [t for t in all_trades if t["ticker"] == ticker]
    if not trades:
        return jsonify({"error": f"No trades found for {ticker}"})

    buys = [t for t in trades if t["trade_type"] == "buy"]
    sells = [t for t in trades if t["trade_type"] == "sell"]

    buy_pols = set(t["politician"] for t in buys)
    sell_pols = set(t["politician"] for t in sells)
    buy_cap = sum(midpoint(t["amount_low"], t["amount_high"]) for t in buys)
    sell_cap = sum(midpoint(t["amount_low"], t["amount_high"]) for t in sells)

    return jsonify({
        "ticker": ticker,
        "price": prices.get(ticker),
        "total_buys": len(buys),
        "total_sells": len(sells),
        "buy_capital": round(buy_cap),
        "sell_capital": round(sell_cap),
        "net_capital": round(buy_cap - sell_cap),
        "direction": "bullish" if buy_cap > sell_cap else "bearish" if sell_cap > buy_cap else "neutral",
        "buyers": [{"name": p, "score": score_map.get(p, {}).get("score", 0)} for p in sorted(buy_pols)],
        "sellers": [{"name": p, "score": score_map.get(p, {}).get("score", 0)} for p in sorted(sell_pols)],
        "recent_trades": [{
            "date": t["trade_date"], "politician": t["politician"],
            "type": t["trade_type"], "amount": fmt_amount(t["amount_low"], t["amount_high"]),
        } for t in sorted(trades, key=lambda x: x["trade_date"] or "", reverse=True)[:20]],
    })


@app.route("/api/politician/<name>")
def politician_detail(name):
    """Get full detail on a specific politician — all their trades, score breakdown."""
    leaderboard, all_trades, _ = score_all()
    # Fuzzy match: case-insensitive substring
    matches = [r for r in leaderboard if name.lower() in r["politician"].lower()]
    if not matches:
        return jsonify({"error": f"No politician matching '{name}'"})

    pol = matches[0]
    trades = [t for t in all_trades if t["politician"] == pol["politician"]]
    prices = get_prices(list(set(t["ticker"] for t in trades if t["ticker"])))

    return jsonify({
        **pol,
        "recent_trades": [{
            "date": t["trade_date"], "ticker": t["ticker"],
            "type": t["trade_type"], "amount": fmt_amount(t["amount_low"], t["amount_high"]),
            "asset": t["asset_name"], "price": prices.get(t["ticker"]),
        } for t in sorted(trades, key=lambda x: x["trade_date"] or "", reverse=True)[:30]],
    })


@app.route("/")
def index():
    return send_from_directory("static", "index.html")

if __name__ == "__main__":
    import sys
    if "--export" in sys.argv:
        # Export API data as static JSON for Firebase/static hosting
        with app.test_client() as c:
            resp = c.get("/api/briefing")
            data = resp.data.decode()
        out = BASE_DIR / "static" / "data.json"
        with open(out, "w") as f:
            f.write(data)
        print(f"Exported dashboard data to {out}")
        print(f"Deploy the static/ folder to Firebase Hosting.")
    else:
        print(f"\n  Congress Trades Dashboard")
        print(f"  http://localhost:5000")
        print(f"  Run with --export to generate static JSON for hosting\n")
        app.run(debug=False, port=5000)
