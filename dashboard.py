#!/usr/bin/env python3
"""
Congress Trades Dashboard
=========================
Local Streamlit app that reads from the SQLite database and displays:
  1. Leaderboard of top political traders (scored across 4 dimensions)
  2. Recent buy feed filtered to top performers
  3. "Stocks to Watch" — tickers bought by multiple politicians (configurable window)

Run:  streamlit run dashboard.py
"""

import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress_trades.db"

SCORING_WEIGHTS = {
    "return": 0.35,
    "win_rate": 0.25,
    "size": 0.20,
    "recency": 0.20,
}

SIZE_LABELS = {
    (1001, 15000): "$1K–$15K",
    (15001, 50000): "$15K–$50K",
    (50001, 100000): "$50K–$100K",
    (100001, 250000): "$100K–$250K",
    (250001, 500000): "$250K–$500K",
    (500001, 1000000): "$500K–$1M",
    (1000001, 5000000): "$1M–$5M",
    (5000001, 25000000): "$5M–$25M",
    (25000001, 50000000): "$25M–$50M",
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_connection():
    """Return a persistent database connection."""
    if not DB_PATH.exists():
        st.error(f"Database not found at `{DB_PATH}`. Run the scraper first.")
        st.stop()
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=300)
def load_all_trades():
    """Load every trade into a DataFrame."""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT politician, chamber, state, trade_date, filing_date,
               ticker, asset_name, trade_type, amount_low, amount_high,
               amount_raw, owner, source_url
        FROM trades
        ORDER BY trade_date DESC
    """, conn)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    return df


def format_amount(low, high):
    """Turn amount bounds into a readable range string."""
    for (lo, hi), label in SIZE_LABELS.items():
        if low == lo:
            return label
    if pd.notna(low) and pd.notna(high):
        return f"${low:,.0f}–${high:,.0f}"
    if pd.notna(low):
        return f"${low:,.0f}+"
    return "N/A"


# ---------------------------------------------------------------------------
# Scoring (self-contained, no yfinance — pure heuristic off scraped data)
# ---------------------------------------------------------------------------
def _position_midpoint(low, high):
    if pd.notna(low) and pd.notna(high):
        return (low + high) / 2
    if pd.notna(low):
        return low * 1.5
    return 8000


def score_politicians(df: pd.DataFrame, lookback_days: int = 730,
                      min_trades: int = 3) -> pd.DataFrame:
    """Score every politician. Returns a DataFrame sorted by composite score."""
    latest_date = df["trade_date"].max()
    if pd.isna(latest_date):
        return pd.DataFrame()
    cutoff = latest_date - timedelta(days=lookback_days)
    window = df[df["trade_date"] >= cutoff].copy()

    records = []
    for politician, group in window.groupby("politician"):
        if len(group) < min_trades:
            continue

        buys = group[group["trade_type"] == "buy"]
        sells = group[group["trade_type"] == "sell"]
        all_tickers = set(group["ticker"].dropna())

        # --- Return score (heuristic: buy/sell pair matching) ---
        returns = []
        for _, buy in buys.iterrows():
            t = buy["ticker"]
            if pd.isna(t) or pd.isna(buy["trade_date"]):
                continue
            later_sells = sells[
                (sells["ticker"] == t) &
                (sells["trade_date"] > buy["trade_date"])
            ]
            if not later_sells.empty:
                sell = later_sells.iloc[0]
                sell_mid = _position_midpoint(sell["amount_low"], sell["amount_high"])
                buy_mid = _position_midpoint(buy["amount_low"], buy["amount_high"])
                ret = (sell_mid - buy_mid) / buy_mid if buy_mid > 0 else 0.05
                ret = max(-0.5, min(1.0, ret))
                returns.append((ret, buy_mid))
            else:
                returns.append((0.05, _position_midpoint(buy["amount_low"], buy["amount_high"])))

        if returns:
            total_w = sum(s for _, s in returns)
            avg_ret = sum(r * s for r, s in returns) / total_w if total_w > 0 else 0
            return_score = max(0, min(100, 50 + avg_ret * 100))
            est_return_pct = round(avg_ret * 100, 1)
        else:
            return_score = 25.0
            est_return_pct = None

        # --- Win rate ---
        wins, losses, scored = 0, 0, 0
        for _, buy in buys.iterrows():
            t = buy["ticker"]
            if pd.isna(t) or pd.isna(buy["trade_date"]):
                continue
            later_sells = sells[
                (sells["ticker"] == t) &
                (sells["trade_date"] > buy["trade_date"])
            ]
            if not later_sells.empty:
                sell = later_sells.iloc[0]
                sell_mid = _position_midpoint(sell["amount_low"], sell["amount_high"])
                buy_mid = _position_midpoint(buy["amount_low"], buy["amount_high"])
                scored += 1
                if sell_mid >= buy_mid:
                    wins += 1
                else:
                    losses += 1
        if scored > 0:
            win_rate = wins / scored
            win_score = win_rate * 100
        else:
            win_rate = None
            win_score = 50.0

        # --- Position size (log-scaled) ---
        sizes = [_position_midpoint(r["amount_low"], r["amount_high"]) for _, r in buys.iterrows()]
        avg_size = sum(sizes) / len(sizes) if sizes else 8000
        total_deployed = sum(sizes)
        log_val = math.log10(max(avg_size, 1000))
        size_score = max(0, min(100, (log_val - 3.9) / (7.9 - 3.9) * 90 + 10))

        # --- Recency ---
        dates = group["trade_date"].dropna()
        if len(dates) > 0:
            latest_trade = dates.max()
            earliest_trade = dates.min()
            days_since = (latest_date - latest_trade).days
            span_months = max(1, (latest_trade - earliest_trade).days / 30)
            trades_per_month = len(dates) / span_months
            decay = math.exp(-0.693 * days_since / 90)
            recency_score = min(100, decay * 100 * 0.7 + min(100, trades_per_month * 10) * 0.3)
        else:
            recency_score = 0
            days_since = None
            trades_per_month = 0
            latest_trade = None

        # --- Composite ---
        composite = (
            return_score * SCORING_WEIGHTS["return"]
            + win_score * SCORING_WEIGHTS["win_rate"]
            + size_score * SCORING_WEIGHTS["size"]
            + recency_score * SCORING_WEIGHTS["recency"]
        )

        records.append({
            "Politician": politician,
            "State": group["state"].iloc[0] or "",
            "Score": round(composite, 1),
            "Return": round(return_score, 1),
            "Win Rate": round(win_score, 1),
            "Size": round(size_score, 1),
            "Recency": round(recency_score, 1),
            "Est. Return %": est_return_pct,
            "Win %": f"{win_rate*100:.0f}%" if win_rate is not None else "N/A",
            "Avg Position": f"${avg_size:,.0f}",
            "Total Deployed": f"${total_deployed:,.0f}",
            "Buys": len(buys),
            "Sells": len(sells),
            "Tickers": len(all_tickers),
            "Trades/Mo": round(trades_per_month, 1),
            "Last Trade": latest_trade.strftime("%Y-%m-%d") if pd.notna(latest_trade) else "N/A",
            "_avg_size_raw": avg_size,
            "_composite_raw": composite,
        })

    result = pd.DataFrame(records).sort_values("Score", ascending=False).reset_index(drop=True)
    result.index = result.index + 1
    result.index.name = "Rank"
    return result


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(
        page_title="Congress Trades",
        page_icon="🏛️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # --- Custom styling ---
    st.markdown("""
    <style>
    /* Tighten metric cards */
    [data-testid="stMetric"] {
        background: #0e1117;
        border: 1px solid #262730;
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetric"] label { font-size: 0.8rem; }
    /* Make tables more compact */
    .stDataFrame { font-size: 0.85rem; }
    div[data-testid="stExpander"] { border: 1px solid #262730; border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

    st.title("Congress Trades Dashboard")

    # Load data
    df = load_all_trades()
    if df.empty:
        st.warning("No trades in the database. Run the scraper first.")
        return

    latest = df["trade_date"].max()
    earliest = df["trade_date"].min()

    # --- Sidebar ---
    st.sidebar.header("Filters")
    st.sidebar.caption(f"Data: {earliest.strftime('%b %d, %Y')} — {latest.strftime('%b %d, %Y')}")
    house_count = len(df[df["chamber"] == "House"])
    senate_count = len(df[df["chamber"] == "Senate"])
    st.sidebar.caption(
        f"{len(df):,} trades from {df['politician'].nunique()} politicians"
        f" ({house_count:,} House, {senate_count:,} Senate)"
    )

    score_threshold = st.sidebar.slider("Min Score for Buy Signals", 0, 100, 50, 5)
    min_trades = st.sidebar.slider("Min Trades to Score", 1, 20, 3)
    watch_window = st.sidebar.selectbox("Stocks to Watch Window", [30, 60, 90, 180, 365], index=2)
    min_buyers = st.sidebar.slider("Min Buyers for Watch List", 2, 5, 2)

    # Score politicians
    leaderboard = score_politicians(df, min_trades=min_trades)

    if leaderboard.empty:
        st.warning("No politicians meet the minimum trade threshold.")
        return

    # --- Top-line metrics ---
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Trades", f"{len(df):,}")
    col2.metric("Politicians", df["politician"].nunique())
    buy_count = len(df[df["trade_type"] == "buy"])
    col3.metric("Total Buys", f"{buy_count:,}")
    col4.metric("Unique Tickers", df["ticker"].dropna().nunique())
    top_score = leaderboard["Score"].iloc[0] if len(leaderboard) > 0 else 0
    col5.metric("Top Score", f"{top_score:.1f}")

    # ===================================================================
    # TAB LAYOUT
    # ===================================================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Leaderboard", "Buy Signal Feed", "Stocks to Watch", "Idea Generator", "Wrapped"])

    # --- TAB 1: LEADERBOARD ---
    with tab1:
        st.subheader("Politician Leaderboard")
        st.caption("Scored on estimated return (35%), win rate (25%), position size (20%), and recency (20%)")

        display_cols = [
            "Politician", "State", "Score", "Return", "Win Rate", "Size",
            "Recency", "Buys", "Sells", "Tickers", "Last Trade"
        ]

        st.dataframe(
            leaderboard[display_cols],
            use_container_width=True,
            height=min(800, 40 + 35 * len(leaderboard)),
            column_config={
                "Score": st.column_config.ProgressColumn(
                    "Score", min_value=0, max_value=100, format="%.1f"
                ),
                "Return": st.column_config.NumberColumn("Return", format="%.1f"),
                "Win Rate": st.column_config.NumberColumn("Win Rate", format="%.1f"),
                "Size": st.column_config.NumberColumn("Size", format="%.1f"),
                "Recency": st.column_config.NumberColumn("Recency", format="%.1f"),
            },
        )

        # Expandable details for top 5
        st.markdown("---")
        st.subheader("Top Performer Details")
        for _, row in leaderboard.head(5).iterrows():
            with st.expander(f"**{row['Politician']}** ({row['State']}) — Score: {row['Score']}"):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Est. Return",
                          f"{row['Est. Return %']:+.1f}%" if row['Est. Return %'] is not None else "N/A")
                c2.metric("Win Rate", row["Win %"])
                c3.metric("Avg Position", row["Avg Position"])
                c4.metric("Total Deployed", row["Total Deployed"])

                c5, c6, c7, c8 = st.columns(4)
                c5.metric("Buys", row["Buys"])
                c6.metric("Sells", row["Sells"])
                c7.metric("Unique Tickers", row["Tickers"])
                c8.metric("Trades/Month", row["Trades/Mo"])

                # Their recent trades
                pol_trades = df[df["politician"] == row["Politician"]].head(15)
                if not pol_trades.empty:
                    st.caption("Recent trades")
                    show = pol_trades[["trade_date", "ticker", "trade_type",
                                       "amount_raw", "asset_name"]].copy()
                    show.columns = ["Date", "Ticker", "Type", "Amount", "Asset"]
                    show["Date"] = show["Date"].dt.strftime("%Y-%m-%d")
                    st.dataframe(show, use_container_width=True, hide_index=True)

    # --- TAB 2: BUY SIGNAL FEED ---
    with tab2:
        st.subheader("Buy Signal Feed")
        st.caption(f"Recent buys from politicians scoring ≥ {score_threshold}")

        qualified = set(leaderboard[leaderboard["Score"] >= score_threshold]["Politician"])

        if not qualified:
            st.info(f"No politicians score above {score_threshold}. Lower the threshold in the sidebar.")
        else:
            st.caption(f"{len(qualified)} qualified politician(s)")

            buys = df[
                (df["trade_type"] == "buy") &
                (df["politician"].isin(qualified)) &
                (df["ticker"].notna())
            ].copy()

            if buys.empty:
                st.info("No buy signals found.")
            else:
                # Merge scores
                score_map = leaderboard.set_index("Politician")["Score"].to_dict()
                buys["Score"] = buys["politician"].map(score_map)
                buys["Amount"] = buys.apply(
                    lambda r: format_amount(r["amount_low"], r["amount_high"]), axis=1
                )
                buys["Date"] = buys["trade_date"].dt.strftime("%Y-%m-%d")

                display = buys[["Date", "politician", "Score", "ticker",
                                "Amount", "asset_name", "state"]].copy()
                display.columns = ["Date", "Politician", "Score", "Ticker",
                                   "Amount", "Asset", "State"]

                # Ticker filter
                all_tickers = sorted(display["Ticker"].dropna().unique())
                selected_ticker = st.selectbox(
                    "Filter by ticker", ["All"] + all_tickers
                )
                if selected_ticker != "All":
                    display = display[display["Ticker"] == selected_ticker]

                st.dataframe(
                    display,
                    use_container_width=True,
                    height=min(800, 40 + 35 * len(display)),
                    hide_index=True,
                    column_config={
                        "Score": st.column_config.NumberColumn("Score", format="%.1f"),
                    },
                )
                st.caption(f"{len(display)} buy signal(s)")

    # --- TAB 3: STOCKS TO WATCH ---
    with tab3:
        st.subheader("Stocks to Watch")
        st.caption(
            f"Tickers bought by {min_buyers}+ politicians in the last "
            f"{watch_window} days of data"
        )

        watch_cutoff = latest - timedelta(days=watch_window)
        watch_buys = df[
            (df["trade_type"] == "buy") &
            (df["ticker"].notna()) &
            (df["trade_date"] >= watch_cutoff)
        ]

        if watch_buys.empty:
            st.info("No buys in this window.")
        else:
            # Group by ticker
            clusters = (
                watch_buys.groupby("ticker")
                .agg(
                    Buyers=("politician", "nunique"),
                    Politicians=("politician", lambda x: ", ".join(sorted(set(x)))),
                    Total_Buys=("politician", "count"),
                    Last_Buy=("trade_date", "max"),
                    Avg_Amount=("amount_low", lambda x: x.dropna().mean()),
                )
                .reset_index()
            )
            clusters.columns = ["Ticker", "Buyers", "Politicians", "Total Buys",
                                "Last Buy", "Avg Low Amount"]
            clusters = clusters[clusters["Buyers"] >= min_buyers].sort_values(
                ["Buyers", "Total Buys"], ascending=[False, False]
            ).reset_index(drop=True)

            if clusters.empty:
                st.info(
                    f"No tickers bought by {min_buyers}+ politicians in the last "
                    f"{watch_window} days. Try a wider window or lower the minimum."
                )
            else:
                st.markdown(f"**{len(clusters)}** ticker(s) with multi-politician buying activity")

                for _, row in clusters.iterrows():
                    score_map = leaderboard.set_index("Politician")["Score"].to_dict()
                    pols = [p.strip() for p in row["Politicians"].split(",")]

                    with st.expander(
                        f"**{row['Ticker']}** — {row['Buyers']} buyers, "
                        f"{row['Total Buys']} total buys"
                    ):
                        # Show who bought
                        pol_data = []
                        for p in pols:
                            p_score = score_map.get(p, "N/A")
                            p_buys = watch_buys[
                                (watch_buys["ticker"] == row["Ticker"]) &
                                (watch_buys["politician"] == p)
                            ]
                            last = p_buys["trade_date"].max()
                            last_str = last.strftime("%Y-%m-%d") if pd.notna(last) else "N/A"
                            amounts = p_buys.apply(
                                lambda r: format_amount(r["amount_low"], r["amount_high"]),
                                axis=1
                            ).tolist()
                            pol_data.append({
                                "Politician": p,
                                "Score": p_score if isinstance(p_score, (int, float)) else "N/A",
                                "Buys": len(p_buys),
                                "Last Buy": last_str,
                                "Amounts": ", ".join(amounts[:3]) + ("..." if len(amounts) > 3 else ""),
                            })
                        pol_df = pd.DataFrame(pol_data)
                        st.dataframe(pol_df, use_container_width=True, hide_index=True)

                        # Individual transactions
                        ticker_trades = watch_buys[
                            watch_buys["ticker"] == row["Ticker"]
                        ][["trade_date", "politician", "amount_raw", "asset_name"]].copy()
                        ticker_trades.columns = ["Date", "Politician", "Amount", "Asset"]
                        ticker_trades["Date"] = ticker_trades["Date"].dt.strftime("%Y-%m-%d")
                        st.caption("Individual transactions")
                        st.dataframe(ticker_trades, use_container_width=True, hide_index=True)

                # Summary table
                st.markdown("---")
                st.subheader("Summary Table")
                summary = clusters[["Ticker", "Buyers", "Total Buys", "Last Buy"]].copy()
                summary["Last Buy"] = summary["Last Buy"].dt.strftime("%Y-%m-%d")
                st.dataframe(summary, use_container_width=True, hide_index=True)

    # --- TAB 4: IDEA GENERATOR ---
    with tab4:
        st.subheader("Stock Idea Generator")
        st.caption(
            "Randomly surfaces tickers from the recent buy pool, weighted by "
            "how many politicians bought them, how recently, and their scores"
        )

        # --- Controls ---
        gen_col1, gen_col2, gen_col3 = st.columns(3)
        n_ideas = gen_col1.slider("Number of ideas", 1, 20, 5, key="n_ideas")
        gen_window = gen_col2.selectbox(
            "Lookback window (days)",
            [30, 60, 90, 180, 365],
            index=2,
            key="gen_window",
        )
        gen_min_score = gen_col3.slider(
            "Min politician score", 0, 100, 0, 5, key="gen_min_score",
            help="Only include buys from politicians at or above this score"
        )

        # --- Build the weighted ticker pool ---
        gen_cutoff = latest - timedelta(days=gen_window)
        score_map = leaderboard.set_index("Politician")["Score"].to_dict()

        gen_buys = df[
            (df["trade_type"] == "buy") &
            (df["ticker"].notna()) &
            (df["trade_date"] >= gen_cutoff)
        ].copy()

        # Optionally filter to scored politicians above threshold
        if gen_min_score > 0:
            qualified_pols = {p for p, s in score_map.items() if s >= gen_min_score}
            gen_buys = gen_buys[gen_buys["politician"].isin(qualified_pols)]

        if gen_buys.empty:
            st.info("No buys in this window. Try a wider lookback or lower the min score.")
        else:
            # Compute per-ticker weight
            ticker_stats = []
            for ticker, grp in gen_buys.groupby("ticker"):
                n_buyers = grp["politician"].nunique()
                n_buys = len(grp)
                last_buy = grp["trade_date"].max()
                days_ago = (latest - last_buy).days
                # Recency factor: exponential decay, half-life 45 days
                recency_factor = math.exp(-0.693 * days_ago / 45)
                # Average politician score for buyers of this ticker
                buyer_scores = [
                    score_map.get(p, 30) for p in grp["politician"].unique()
                ]
                avg_pol_score = sum(buyer_scores) / len(buyer_scores)
                # Total estimated position (sum of midpoints)
                total_position = sum(
                    _position_midpoint(r["amount_low"], r["amount_high"])
                    for _, r in grp.iterrows()
                )

                # Composite weight:
                #   buyers^1.5  (strong bonus for multi-politician convergence)
                #   * log(n_buys+1)  (more buys = more signal, but diminishing)
                #   * recency_factor  (recent >> stale)
                #   * (avg_pol_score / 50)  (better politicians = heavier weight)
                weight = (
                    (n_buyers ** 1.5)
                    * math.log(n_buys + 1)
                    * recency_factor
                    * (avg_pol_score / 50)
                )

                ticker_stats.append({
                    "ticker": ticker,
                    "n_buyers": n_buyers,
                    "n_buys": n_buys,
                    "last_buy": last_buy,
                    "days_ago": days_ago,
                    "recency_factor": recency_factor,
                    "avg_pol_score": avg_pol_score,
                    "total_position": total_position,
                    "weight": weight,
                })

            pool = pd.DataFrame(ticker_stats)
            pool = pool[pool["weight"] > 0].copy()

            if pool.empty:
                st.info("No tickers in the weighted pool.")
            else:
                # Normalize weights to probabilities
                pool["prob"] = pool["weight"] / pool["weight"].sum()
                pool = pool.sort_values("weight", ascending=False).reset_index(drop=True)

                st.caption(
                    f"{len(pool)} tickers in pool from {gen_buys['politician'].nunique()} "
                    f"politician(s) | Top-weighted: {pool.iloc[0]['ticker']} "
                    f"({pool.iloc[0]['prob']*100:.1f}% draw chance)"
                )

                # Session state for persisting results across reruns
                if "gen_seed" not in st.session_state:
                    st.session_state.gen_seed = None
                if "gen_results" not in st.session_state:
                    st.session_state.gen_results = None

                if st.button("Generate Ideas", type="primary", use_container_width=True):
                    # New seed each click
                    st.session_state.gen_seed = int(datetime.now().timestamp() * 1000)
                    rng = np.random.default_rng(st.session_state.gen_seed)
                    actual_n = min(n_ideas, len(pool))
                    picked_indices = rng.choice(
                        len(pool), size=actual_n, replace=False, p=pool["prob"].values
                    )
                    st.session_state.gen_results = pool.iloc[picked_indices]["ticker"].tolist()

                if st.session_state.gen_results:
                    st.markdown("---")
                    for i, ticker in enumerate(st.session_state.gen_results, 1):
                        t_row = pool[pool["ticker"] == ticker].iloc[0]
                        t_buys = gen_buys[gen_buys["ticker"] == ticker].sort_values(
                            "trade_date", ascending=False
                        )

                        # Header card
                        st.markdown(f"### {i}. {ticker}")

                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Buyers", int(t_row["n_buyers"]))
                        m2.metric("Total Buys", int(t_row["n_buys"]))
                        m3.metric(
                            "Last Buy",
                            t_row["last_buy"].strftime("%Y-%m-%d")
                            if pd.notna(t_row["last_buy"]) else "N/A"
                        )
                        m4.metric(
                            "Est. Total Position",
                            f"${t_row['total_position']:,.0f}"
                        )

                        m5, m6, m7, _ = st.columns(4)
                        m5.metric("Avg Politician Score", f"{t_row['avg_pol_score']:.1f}")
                        m6.metric("Draw Weight", f"{t_row['prob']*100:.1f}%")
                        m7.metric("Days Since Last Buy", int(t_row["days_ago"]))

                        # Transaction detail table
                        detail = t_buys[[
                            "trade_date", "politician", "amount_low", "amount_high",
                            "amount_raw", "asset_name"
                        ]].copy()
                        detail["Score"] = detail["politician"].map(score_map).round(1)
                        detail["Est. Position"] = detail.apply(
                            lambda r: f"${_position_midpoint(r['amount_low'], r['amount_high']):,.0f}",
                            axis=1
                        )
                        detail["Amount"] = detail.apply(
                            lambda r: format_amount(r["amount_low"], r["amount_high"]),
                            axis=1
                        )
                        detail["Date"] = detail["trade_date"].dt.strftime("%Y-%m-%d")
                        detail = detail[[
                            "Date", "politician", "Score", "Amount",
                            "Est. Position", "asset_name"
                        ]]
                        detail.columns = [
                            "Date", "Politician", "Score", "Amount",
                            "Est. Position", "Asset"
                        ]

                        st.dataframe(
                            detail,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Score": st.column_config.NumberColumn(
                                    "Score", format="%.1f"
                                ),
                            },
                        )
                        st.markdown("")

                # Show the full weight distribution in an expander
                with st.expander("View full ticker weight distribution"):
                    weight_display = pool[[
                        "ticker", "n_buyers", "n_buys", "days_ago",
                        "avg_pol_score", "total_position", "weight", "prob"
                    ]].copy()
                    weight_display.columns = [
                        "Ticker", "Buyers", "Buys", "Days Ago",
                        "Avg Score", "Total Position", "Weight", "Draw %"
                    ]
                    weight_display["Total Position"] = weight_display["Total Position"].apply(
                        lambda x: f"${x:,.0f}"
                    )
                    weight_display["Avg Score"] = weight_display["Avg Score"].round(1)
                    weight_display["Weight"] = weight_display["Weight"].round(2)
                    weight_display["Draw %"] = (weight_display["Draw %"] * 100).round(2)

                    st.dataframe(
                        weight_display,
                        use_container_width=True,
                        height=min(600, 40 + 35 * len(weight_display)),
                        hide_index=True,
                    )

    # --- TAB 5: WRAPPED ---
    with tab5:
        st.subheader("Congressional Trading Wrapped")
        st.caption(
            f"A complete snapshot of insider trading activity "
            f"({earliest.strftime('%b %Y')} — {latest.strftime('%b %Y')})"
        )

        # --- Top Stats Row ---
        w1, w2, w3, w4 = st.columns(4)
        total_buys = df[df["trade_type"] == "buy"]
        total_sells = df[df["trade_type"] == "sell"]

        # Estimate total capital deployed
        total_capital = sum(
            _position_midpoint(r["amount_low"], r["amount_high"])
            for _, r in total_buys.iterrows()
        )
        w1.metric("Total Capital Deployed (est.)", f"${total_capital:,.0f}")
        w2.metric("Buy/Sell Ratio", f"{len(total_buys) / max(len(total_sells), 1):.2f}")

        # Busiest month
        month_counts = df.set_index("trade_date").resample("ME").size()
        if not month_counts.empty:
            busiest = month_counts.idxmax()
            w3.metric("Busiest Month", busiest.strftime("%b %Y") if pd.notna(busiest) else "N/A")
        else:
            w3.metric("Busiest Month", "N/A")

        # Most traded ticker
        top_ticker = df["ticker"].dropna().value_counts()
        if not top_ticker.empty:
            w4.metric("Most Traded Ticker", f"{top_ticker.index[0]} ({top_ticker.iloc[0]}x)")

        st.markdown("---")

        # --- Most Active Trader ---
        st.markdown("### Most Active Insider Trader")
        if not leaderboard.empty:
            top = leaderboard.iloc[0]
            tc1, tc2, tc3, tc4 = st.columns(4)
            tc1.metric("Name", top["Politician"])
            tc2.metric("Composite Score", f"{top['Score']:.1f}")
            tc3.metric("Total Buys", int(top["Buys"]))
            tc4.metric("Unique Tickers", int(top["Tickers"]))

        # --- Top 5 "Insider Traders" ---
        st.markdown("---")
        st.markdown("### Top 5 Politicians by Insider Score")
        for idx, (_, row) in enumerate(leaderboard.head(5).iterrows(), 1):
            cols = st.columns([0.5, 3, 1, 1, 1, 1])
            cols[0].markdown(f"### {idx}")
            cols[1].markdown(f"**{row['Politician']}** ({row['State']})")
            cols[2].metric("Score", f"{row['Score']:.1f}")
            cols[3].metric("Return", f"{row['Return']:.0f}")
            cols[4].metric("Win Rate", f"{row['Win Rate']:.0f}")
            cols[5].metric("Buys", int(row["Buys"]))

        # --- Sector / Ticker Concentration ---
        st.markdown("---")
        st.markdown("### Most Bought Tickers (All Politicians)")
        buy_tickers = total_buys["ticker"].dropna().value_counts().head(15)
        if not buy_tickers.empty:
            ticker_df = buy_tickers.reset_index()
            ticker_df.columns = ["Ticker", "Buys"]
            # Add buyer count and total position estimate
            enriched = []
            for _, r in ticker_df.iterrows():
                t = r["Ticker"]
                t_buys = total_buys[total_buys["ticker"] == t]
                n_buyers = t_buys["politician"].nunique()
                est_pos = sum(
                    _position_midpoint(row["amount_low"], row["amount_high"])
                    for _, row in t_buys.iterrows()
                )
                enriched.append({
                    "Ticker": t,
                    "Total Buys": int(r["Buys"]),
                    "Distinct Buyers": n_buyers,
                    "Est. Capital": f"${est_pos:,.0f}",
                    "Buyers": ", ".join(sorted(t_buys["politician"].unique())[:5]),
                })
            st.dataframe(
                pd.DataFrame(enriched),
                use_container_width=True,
                hide_index=True,
            )

        # --- Biggest Single Trades ---
        st.markdown("---")
        st.markdown("### Biggest Single Trades")
        big_trades = df.nlargest(10, "amount_low", keep="first")[
            ["trade_date", "politician", "ticker", "trade_type",
             "amount_raw", "asset_name"]
        ].copy()
        big_trades["trade_date"] = big_trades["trade_date"].dt.strftime("%Y-%m-%d")
        big_trades.columns = ["Date", "Politician", "Ticker", "Type", "Amount", "Asset"]
        st.dataframe(big_trades, use_container_width=True, hide_index=True)

        # --- Convergence Highlight ---
        st.markdown("---")
        st.markdown("### Convergence Hotspots")
        st.caption("Tickers bought by the most distinct politicians")
        convergence = (
            total_buys[total_buys["ticker"].notna()]
            .groupby("ticker")
            .agg(
                Buyers=("politician", "nunique"),
                Total_Buys=("politician", "count"),
                Politicians=("politician", lambda x: ", ".join(sorted(set(x))[:5])),
            )
            .reset_index()
        )
        convergence.columns = ["Ticker", "Buyers", "Total Buys", "Politicians"]
        convergence = convergence[convergence["Buyers"] >= 2].sort_values(
            "Buyers", ascending=False
        ).head(10).reset_index(drop=True)
        if convergence.empty:
            st.info("No convergence detected (need 2+ politicians buying the same ticker).")
        else:
            st.dataframe(convergence, use_container_width=True, hide_index=True)

        # --- Trading Pace ---
        st.markdown("---")
        st.markdown("### Trading Pace Over Time")
        pace = df.set_index("trade_date").resample("ME").agg(
            Trades=("politician", "count"),
            Buys=("trade_type", lambda x: (x == "buy").sum()),
            Sells=("trade_type", lambda x: (x == "sell").sum()),
        ).reset_index()
        pace.columns = ["Month", "Total Trades", "Buys", "Sells"]
        pace["Month"] = pace["Month"].dt.strftime("%Y-%m")
        st.bar_chart(pace.set_index("Month")[["Buys", "Sells"]])

        # --- Chamber Breakdown ---
        if senate_count > 0:
            st.markdown("---")
            st.markdown("### House vs Senate")
            hc, sc = st.columns(2)
            hc.metric("House Trades", f"{house_count:,}")
            hc.metric("House Politicians", df[df["chamber"] == "House"]["politician"].nunique())
            sc.metric("Senate Trades", f"{senate_count:,}")
            sc.metric("Senate Politicians", df[df["chamber"] == "Senate"]["politician"].nunique())


if __name__ == "__main__":
    main()
