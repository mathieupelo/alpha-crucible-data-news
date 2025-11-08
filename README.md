# Alpha Crucible Data - News

Fetches yfinance news data and stores it in the ORE database (copper schema).

## Overview

This repository is responsible for:
1. Fetching news data from yfinance API for all tickers in the database
2. Filtering news by publication date
3. Storing the data in the ORE database (`copper.yfinance_news` table)

## How It Works

For each date in the specified range:

1. **Fetch all distinct tickers** from the `universe_tickers` table in the main database
2. **Check processing status** - verify if each ticker has already been processed for that date
3. **Fetch news** - For unprocessed tickers, fetch all news from yfinance API
4. **Filter by date** - Only keep news items published on the target date
5. **Insert into database** - Store news in `copper.yfinance_news` table

## Database Schema

The script creates the following table in the ORE database:

```sql
CREATE TABLE copper.yfinance_news (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    title TEXT,
    summary TEXT,
    publisher VARCHAR(255),
    link TEXT,
    published_date TIMESTAMP,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, link, published_date)
);
```

## Setup

1. Clone this repository
2. Copy `.env.example` to `.env` and fill in your credentials
3. Install dependencies: `pip install -r requirements.txt`
4. Run: `python main.py`

## Docker

Build and run with Docker:

```bash
docker build -t alpha-crucible-data-news .
docker run --env-file .env alpha-crucible-data-news
```

## Environment Variables

### Required Database Connections

**Main Database** (for fetching tickers):
- `DATABASE_URL` (recommended) OR
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`

**ORE Database** (for storing news):
- `ORE_DATABASE_URL` (recommended) OR
- `ORE_DB_HOST`, `ORE_DB_PORT`, `ORE_DB_USER`, `ORE_DB_PASSWORD`, `ORE_DB_NAME`

### Date Range (Optional)

- `START_DATE`: Start date in format `YYYY-MM-DD` (defaults to today if not set)
- `END_DATE`: End date in format `YYYY-MM-DD` (defaults to today if not set)

**Note**: For daily Airflow runs, you can set these in the DAG configuration to process a specific date or date range.

## Usage Examples

### Daily Run (Today's News)

```bash
# No date variables needed - defaults to today
python main.py
```

### Specific Date Range

```bash
# Set environment variables
export START_DATE=2025-01-01
export END_DATE=2025-01-31
python main.py
```

### Docker with Date Range

```bash
docker run --env-file .env \
  -e START_DATE=2025-01-01 \
  -e END_DATE=2025-01-31 \
  alpha-crucible-data-news
```

## Exit Codes

- `0`: Success
- `1`: Failure

## Error Handling

- If fetching news for a ticker fails, the script logs the error clearly and continues with the next ticker
- All errors are logged with full stack traces for debugging
- The script will not skip tickers due to connection errors - it will retry or fail gracefully

## Notes

- The script checks if a ticker has already been processed for a date before fetching
- Duplicate news items (same ticker, link, published_date) are automatically skipped
- News is filtered to only include items published on the exact target date
- All operations are logged with clear messages indicating progress and errors

## Development

The main logic is in `main.py`. Key functions:

- `get_distinct_tickers()`: Fetches all tickers from main database
- `is_ticker_processed()`: Checks if ticker/date combination already exists
- `fetch_yfinance_news()`: Fetches news from yfinance API
- `filter_news_by_date()`: Filters news to target date
- `insert_news()`: Bulk inserts news into ORE database
