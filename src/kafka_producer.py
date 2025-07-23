import json
import time
import schedule
import yfinance as yf
from kafka import KafkaProducer
from src.config import KAFKA_BROKER, KAFKA_TOPIC
from src.logger import get_logger

logger = get_logger(__name__)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TICKERS = ['AAPL', 'GOOGL', 'MSFT']

def fetch_and_publish():
    logger.info("Fetching market data...")
    for ticker in TICKERS:
        try:
            data = yf.Ticker(ticker)
            quote = data.history(period='1d', interval='1m').tail(1)

            if quote.empty:
                logger.warning(f"No data for ticker: {ticker}")
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
            producer.send(KAFKA_TOPIC, value=payload)
            logger.info(f"Published: {payload}")

        except Exception as e:
            logger.exception(f"Error publishing data for {ticker}")

schedule.every(1).minutes.do(fetch_and_publish)

if __name__ == '__main__':
    logger.info("Starting Kafka Producer...")
    fetch_and_publish()
    while True:
        schedule.run_pending()
        time.sleep(1)
