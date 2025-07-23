import pandas as pd
from collections import deque
from src.logger import get_logger

logger = get_logger(__name__)
price_cache = {}
WINDOW = 5

def compute_indicators(data):
    ticker = data['ticker']
    price = data['close']

    if ticker not in price_cache:
        price_cache[ticker] = deque(maxlen=WINDOW)

    price_cache[ticker].append(price)
    prices = list(price_cache[ticker])

    if len(prices) >= WINDOW:
        data['rolling_avg'] = round(pd.Series(prices).mean(), 2)
        data['volatility'] = round(pd.Series(prices).std(), 2)
    else:
        data['rolling_avg'] = None
        data['volatility'] = None

    return data
