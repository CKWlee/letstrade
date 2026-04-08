FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy everything needed
COPY app.py .
COPY congress_trades.py .
COPY config.json .
COPY congress_trades.db .
COPY static/ static/

# Cloud Run sets PORT env var
ENV PORT=8080

# gunicorn: 1 worker, 4 threads, 120s timeout (yfinance can be slow)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 4 --timeout 120 app:app
