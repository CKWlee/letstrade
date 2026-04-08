# letstrade

scrapes U.S. politician stock trade disclosures straight from government sources (house clerk PDFs, senate eFD), scores each politician on how good their trades actually are, and gives you a dashboard + API that tells you what to buy or sell based on what congress is doing with their money.

no paid APIs. everything runs off official public filings.

**[live demo](https://congress-trades-560790382976.us-central1.run.app)**

**[stock picks API](https://congress-trades-560790382976.us-central1.run.app/api/picks?action=buy&n=5)**

---

## what it does

- scrapes house clerk XML indexes + PDF filings, parses trade tables with pdfplumber
- senate eFD too (when their server cooperates — it goes down a lot)
- scores every politician on: estimated returns, win rate, position size conviction, and recency
- ranks them on a composite score (35% return, 25% win rate, 20% size, 20% recency)
- detects convergence — multiple politicians independently buying the same stock (strongest signal)
- shows net direction per ticker — are more people buying or selling it
- pulls live stock prices via yfinance
- dashboard shows everything on one page, tiles expand for detail
- API endpoints so you can just curl it and get picks

## the API

```
# top 5 stocks to buy right now
/api/picks?action=buy&n=5

# top 3 to sell
/api/picks?action=sell&n=3

# both, only from politicians scoring above 60
/api/picks?action=both&n=5&min_score=60

# only convergence plays (2+ politicians buying same stock)
/api/picks?action=buy&min_buyers=2

# everything about a specific ticker
/api/ticker/GS

# everything about a specific politician
/api/politician/pelosi

# full dashboard data (what the frontend uses)
/api/briefing
```

## run it locally

```bash
pip install -r requirements.txt

# collect data
python congress_trades.py --years 2024 2025 2026

# run dashboard
python app.py
# http://localhost:5000
```

## how the scoring works

politicians need at least 3 trades to get scored. each gets four sub-scores (0-100):

- **return** — matches buys to later sells on the same ticker, compares the amount ranges. not perfect since congress reports ranges not exact numbers, but it works
- **win rate** — what % of their buys ended up selling at a higher range
- **position size** — log-scaled average buy size. someone dropping $250K+ is more convicted than someone buying $1K-$15K
- **recency** — exponential decay with 90-day half-life. recent traders score higher

the buy signal ranking uses: `buyers^1.5 × log(buys) × recency × (avg_politician_score / 50)`. convergence (multiple politicians, same stock) gets a heavy weight because the academic research says that's where the real signal is.

## deploy to cloud run

```bash
gcloud builds submit --tag gcr.io/YOUR_PROJECT/congress-trades
gcloud run deploy congress-trades \
  --image gcr.io/YOUR_PROJECT/congress-trades \
  --platform managed --region us-central1 \
  --allow-unauthenticated --memory 512Mi \
  --timeout 120 --max-instances 1 --min-instances 0
```

## update data

```bash
python congress_trades.py --years 2024 2025 2026
# then rebuild + redeploy
```

the scraper deduplicates automatically so you can run it as many times as you want without creating duplicate entries.

## project structure

```
app.py                  # flask backend — dashboard + picks API
congress_trades.py      # scraper — house clerk PDFs + senate eFD
static/index.html       # the frontend
config.json             # scoring weights
Dockerfile              # for cloud run
```

## limitations

- congress reports amount ranges, not exact numbers ($1K-$15K, $15K-$50K, etc.)
- 30-45 day filing delay — by the time a trade is public it's already old
- senate eFD goes down randomly, especially on weekends
- PDF parsing handles the standard format but weird layouts might miss trades
- no party affiliation data from house clerk

## data sources

- [house clerk financial disclosures](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/) — primary, fully automated
- [senate eFD](https://efdsearch.senate.gov/search/) — intermittent availability, retries with backoff
