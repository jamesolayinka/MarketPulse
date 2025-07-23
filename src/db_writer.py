
import psycopg2
from psycopg2.extras import RealDictCursor
from src.config import POSTGRES_URI
from src.logger import get_logger

logger = get_logger(__name__)

def write_to_db(data):
    try:
        conn = psycopg2.connect(POSTGRES_URI)
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                ticker TEXT,
                timestamp TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                rolling_avg FLOAT,
                volatility FLOAT
            )
        ''')

        cur.execute('''
            INSERT INTO market_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            data['ticker'], data['timestamp'], data['open'], data['high'],
            data['low'], data['close'], data['volume'],
            data['rolling_avg'], data['volatility']
        ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Inserted data for {data['ticker']} into DB")
    except Exception as e:
        logger.exception("Failed to write to DB")