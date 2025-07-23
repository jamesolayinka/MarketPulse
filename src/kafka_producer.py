import json
import time
from datetime import datetime
from kafka import KafkaProducer
import yfinance as yf
import schedule
from src.logger import get_logger
from src.config import KAFKA_BROKER, KAFKA_TOPIC

logger = logging.getLogger(__name__)

# Initialize Kafka Producer

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka Producer initialized successfully.")
except Exception as e:
    logger.exception("Failed to initialize Kafka Producer.")
    raise e

# Tickers to Monitor

TICKERS = ['AAPL', 'GOOGL', 'MSFT']


# Market Data Fetch & Publish

def fetch_and_publish():
    logger.info("Fetching market data...")

    for ticker in TICKERS:
        try:
            data = yf.Ticker(ticker)
            quote = data.history(period='1d', interval='1m').tail(1)

            if quote.empty:
                logger.warning(f"No data received for ticker: {ticker}")
                continue

            payload = {
                'ticker': ticker,
                'timestamp': quote.index[0].isoformat(),
                'open': round(quote['Open'][0], 2),
                'high': round(quote['High'][0], 2),
                'low': round(quote['Low'][0], 2),
                'close': round(quote['Close'][0], 2),
                'volume': int(quote['Volume'][0])
            }

            producer.send(TOPIC_NAME, value=payload)
            logger.info(f"Published to Kafka: {payload}")

        except Exception as e:
            logger.exception(f"Error fetching or sending data for ticker: {ticker}")

    logger.info("-" * 60)

# Schedule Job

schedule.every(1).minutes.do(fetch_and_publish)


if __name__ == '__main__':
    logger.info("Starting Kafka Producer for financial market data...")
    fetch_and_publish()  # Initial run

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Kafka Producer stopped by user.")
    except Exception as e:
        logger.exception("Unexpected error in main loop.")
