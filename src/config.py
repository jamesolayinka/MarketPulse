import os

KAFKA_BROKER = 'localhost:9092'  # Update with your Kafka broker address
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_data")
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://user:pass@postgres:5432/market")
