#!/usr/bin/env python3
"""
Alpha Crucible Data - News
Fetches yfinance news data and stores it in ORE database (copper schema).
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from datetime import datetime, date, timedelta
from typing import List, Set, Optional, Dict, Any
from dotenv import load_dotenv
import yfinance as yf
import warnings

# Suppress yfinance warnings
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_main_db_connection():
    """Get connection to main database (for fetching tickers)."""
    try:
        # Try DATABASE_URL first
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            logger.info("Connecting to main database using DATABASE_URL")
            return psycopg2.connect(database_url)
        
        # Fall back to individual connection parameters
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '5432')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_NAME')
        
        if not all([host, user, password, database]):
            raise ValueError(
                "Main database connection requires either DATABASE_URL or "
                "(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)"
            )
        
        logger.info(f"Connecting to main database at {host}:{port}")
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
    except Exception as e:
        logger.error(f"Error connecting to main database: {e}")
        raise


def get_ore_db_connection():
    """Get connection to ORE database (for storing news)."""
    try:
        # Try ORE_DATABASE_URL first
        database_url = os.getenv('ORE_DATABASE_URL')
        if not database_url:
            # Try alternative variable name (used in Airflow)
            database_url = os.getenv('DATABASE_ORE_URL')
        
        if database_url:
            logger.info("Connecting to ORE database using database URL")
            return psycopg2.connect(database_url)
        
        # Fall back to individual connection parameters
        host = os.getenv('ORE_DB_HOST')
        port = os.getenv('ORE_DB_PORT', '5432')
        user = os.getenv('ORE_DB_USER')
        password = os.getenv('ORE_DB_PASSWORD')
        database = os.getenv('ORE_DB_NAME')
        
        if not all([host, user, password, database]):
            raise ValueError(
                "ORE database connection requires either ORE_DATABASE_URL, DATABASE_ORE_URL, or "
                "(ORE_DB_HOST, ORE_DB_USER, ORE_DB_PASSWORD, ORE_DB_NAME)"
            )
        
        logger.info(f"Connecting to ORE database at {host}:{port}")
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
    except Exception as e:
        logger.error(f"Error connecting to ORE database: {e}")
        raise


def create_news_table(conn):
    """Create the copper.yfinance_news table if it doesn't exist."""
    try:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS copper;")
            
            # Create table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS copper.yfinance_news (
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
            """)
            
            # Create indexes for better query performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_yfinance_news_ticker_date 
                ON copper.yfinance_news(ticker, DATE(published_date));
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_yfinance_news_published_date 
                ON copper.yfinance_news(published_date);
            """)
            
            conn.commit()
            logger.info("Table copper.yfinance_news created/verified successfully")
            
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
        raise


def get_distinct_tickers(main_conn) -> Set[str]:
    """Get all distinct tickers from universe_tickers table."""
    try:
        with main_conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT ticker FROM universe_tickers ORDER BY ticker;")
            tickers = {row[0] for row in cursor.fetchall()}
            logger.info(f"Found {len(tickers)} distinct tickers in database")
            return tickers
    except Exception as e:
        logger.error(f"Error fetching distinct tickers: {e}")
        raise


def is_ticker_processed(ore_conn, ticker: str, target_date: date) -> bool:
    """Check if ticker has been processed for the given date."""
    try:
        with ore_conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM copper.yfinance_news 
                WHERE ticker = %s 
                AND DATE(published_date) = %s;
            """, (ticker, target_date))
            count = cursor.fetchone()[0]
            return count > 0
    except Exception as e:
        logger.error(f"Error checking if ticker {ticker} processed for {target_date}: {e}")
        # On error, assume not processed to avoid skipping
        return False


def fetch_yfinance_news(ticker: str) -> List[Dict[str, Any]]:
    """Fetch news for a ticker from yfinance."""
    try:
        logger.info(f"Fetching news for ticker: {ticker}")
        stock = yf.Ticker(ticker)
        news_list = stock.news
        
        if not news_list:
            logger.info(f"No news found for {ticker}")
            return []
        
        processed_news = []
        for news_item in news_list:
            try:
                # New yfinance format: data is nested in 'content' field
                content = news_item.get('content')
                if not content:
                    # Fallback to old format (direct fields)
                    content = news_item
                
                # Extract fields from content
                title = content.get('title', '') or ''
                summary = content.get('summary', '') or content.get('description', '') or ''
                publisher = 'Unknown'
                link = ''
                pub_date = None
                image_url = ''
                
                # Handle publisher - check provider field
                provider = content.get('provider')
                if provider:
                    if isinstance(provider, dict):
                        publisher = provider.get('displayName') or provider.get('name', 'Unknown') or 'Unknown'
                    elif provider:
                        publisher = str(provider)
                
                # Handle link/URL - check canonicalUrl or clickThroughUrl
                url_obj = content.get('canonicalUrl') or content.get('clickThroughUrl') or content.get('url')
                if isinstance(url_obj, dict):
                    link = url_obj.get('url', '') or ''
                elif url_obj:
                    link = str(url_obj)
                else:
                    # Fallback to old format
                    link = content.get('link', '') or ''
                
                # Handle image - check thumbnail field
                thumbnail = content.get('thumbnail')
                if thumbnail:
                    if isinstance(thumbnail, dict):
                        image_url = thumbnail.get('originalUrl', '') or thumbnail.get('url', '') or ''
                    elif thumbnail:
                        image_url = str(thumbnail)
                else:
                    # Fallback to old format
                    image_obj = content.get('thumbnailUrl') or content.get('image')
                    if isinstance(image_obj, dict):
                        image_url = image_obj.get('url', '') or image_obj.get('src', '') or ''
                    elif image_obj:
                        image_url = str(image_obj)
                
                # Handle published date - check pubDate field
                pub_date = content.get('pubDate') or content.get('providerPublishTime') or content.get('publishedAt')
                if pub_date:
                    try:
                        if isinstance(pub_date, (int, float)):
                            pub_date = datetime.fromtimestamp(pub_date)
                        else:
                            # Handle ISO format string
                            pub_date_str = str(pub_date).replace('Z', '+00:00')
                            pub_date = datetime.fromisoformat(pub_date_str)
                    except (ValueError, TypeError, OSError) as e:
                        logger.warning(f"Error parsing date for {ticker}: {e}, using current time")
                        pub_date = datetime.now()
                else:
                    pub_date = datetime.now()
                
                # Skip if no meaningful content
                if not title and not summary:
                    continue
                
                processed_news.append({
                    'ticker': ticker,
                    'title': title,
                    'summary': summary,
                    'publisher': publisher,
                    'link': link,
                    'published_date': pub_date,
                    'image_url': image_url
                })
                
            except Exception as e:
                logger.warning(f"Error processing news item for {ticker}: {e}")
                continue
        
        logger.info(f"Fetched {len(processed_news)} news items for {ticker}")
        return processed_news
        
    except Exception as e:
        logger.error(f"ERROR: Failed to fetch news for ticker {ticker}: {e}", exc_info=True)
        return []


def filter_news_by_date(news_list: List[Dict[str, Any]], target_date: date) -> List[Dict[str, Any]]:
    """Filter news to only include items published on the target date."""
    filtered = []
    for news_item in news_list:
        try:
            pub_date = news_item['published_date']
            if isinstance(pub_date, datetime):
                if pub_date.date() == target_date:
                    filtered.append(news_item)
            elif isinstance(pub_date, date):
                if pub_date == target_date:
                    filtered.append(news_item)
        except Exception as e:
            logger.warning(f"Error filtering news by date: {e}")
            continue
    return filtered


def insert_news(ore_conn, news_list: List[Dict[str, Any]]):
    """Insert news items into ORE database."""
    if not news_list:
        return
    
    try:
        with ore_conn.cursor() as cursor:
            # Prepare data for bulk insert
            values = []
            for news in news_list:
                values.append((
                    news['ticker'],
                    news['title'],
                    news['summary'],
                    news['publisher'],
                    news['link'],
                    news['published_date'],
                    news['image_url']
                ))
            
            # Use INSERT ... ON CONFLICT DO NOTHING to handle duplicates
            insert_query = """
                INSERT INTO copper.yfinance_news 
                (ticker, title, summary, publisher, link, published_date, image_url)
                VALUES %s
                ON CONFLICT (ticker, link, published_date) DO NOTHING;
            """
            
            execute_values(cursor, insert_query, values)
            ore_conn.commit()
            
            logger.info(f"Inserted {len(news_list)} news items into database")
            
    except Exception as e:
        logger.error(f"Error inserting news: {e}")
        ore_conn.rollback()
        raise


def process_date_range(start_date: date, end_date: date, main_conn, ore_conn):
    """Process news for all dates in the range."""
    # Get all distinct tickers
    tickers = get_distinct_tickers(main_conn)
    
    if not tickers:
        logger.warning("No tickers found in database. Exiting.")
        return
    
    # Process each date in the range
    current_date = start_date
    total_processed = 0
    total_skipped = 0
    total_errors = 0
    
    while current_date <= end_date:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing date: {current_date}")
        logger.info(f"{'='*60}")
        
        # Get tickers that haven't been processed for this date
        unprocessed_tickers = []
        for ticker in tickers:
            if not is_ticker_processed(ore_conn, ticker, current_date):
                unprocessed_tickers.append(ticker)
            else:
                total_skipped += 1
        
        logger.info(f"Found {len(unprocessed_tickers)} unprocessed tickers for {current_date}")
        logger.info(f"Skipping {total_skipped} already processed tickers")
        
        if not unprocessed_tickers:
            logger.info(f"All tickers already processed for {current_date}. Moving to next date.")
            current_date += timedelta(days=1)
            continue
        
        # Process each unprocessed ticker
        for ticker in unprocessed_tickers:
            try:
                # Fetch news for ticker
                news_list = fetch_yfinance_news(ticker)
                
                if not news_list:
                    logger.info(f"No news found for {ticker} on {current_date}")
                    continue
                
                # Filter to only news from the target date
                filtered_news = filter_news_by_date(news_list, current_date)
                
                if not filtered_news:
                    logger.info(f"No news for {ticker} published on {current_date}")
                    continue
                
                # Insert into database
                insert_news(ore_conn, filtered_news)
                total_processed += len(filtered_news)
                logger.info(f"✓ Successfully processed {len(filtered_news)} news items for {ticker} on {current_date}")
                
            except Exception as e:
                total_errors += 1
                logger.error(f"ERROR: Failed to process ticker {ticker} for date {current_date}: {e}", exc_info=True)
                logger.error(f"Continuing with next ticker...")
                continue
        
        # Move to next date
        current_date += timedelta(days=1)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("PROCESSING SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Total tickers: {len(tickers)}")
    logger.info(f"Total news items processed: {total_processed}")
    logger.info(f"Total tickers skipped (already processed): {total_skipped}")
    logger.info(f"Total errors: {total_errors}")
    logger.info(f"{'='*60}\n")


def get_date_range_from_env() -> tuple[date, date]:
    """Get date range from environment variables."""
    start_date_str = os.getenv('START_DATE')
    end_date_str = os.getenv('END_DATE')
    
    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            logger.info(f"Using date range from environment: {start_date} to {end_date}")
            return start_date, end_date
        except ValueError as e:
            logger.error(f"Invalid date format in environment variables: {e}")
            logger.error("Expected format: YYYY-MM-DD")
            raise
    else:
        # Default to today if not specified
        today = date.today()
        logger.info(f"No date range specified, using today: {today}")
        return today, today


def main():
    """Main execution function."""
    main_conn = None
    ore_conn = None
    
    try:
        logger.info("="*60)
        logger.info("Starting yfinance news data fetch...")
        logger.info("="*60)
        
        # Get date range
        start_date, end_date = get_date_range_from_env()
        
        # Connect to databases
        logger.info("Connecting to databases...")
        main_conn = get_main_db_connection()
        ore_conn = get_ore_db_connection()
        
        # Create table if needed
        logger.info("Creating/verifying table structure...")
        create_news_table(ore_conn)
        
        # Process date range
        process_date_range(start_date, end_date, main_conn, ore_conn)
        
        logger.info("News data fetch completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"FATAL ERROR during execution: {e}", exc_info=True)
        return 1
        
    finally:
        # Close connections
        if main_conn:
            main_conn.close()
            logger.info("Closed main database connection")
        if ore_conn:
            ore_conn.close()
            logger.info("Closed ORE database connection")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
