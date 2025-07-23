import json
from kafka import KafkaConsumer
from src.config import KAFKA_TOPIC, KAFKA_BROKER
from src.logger import get_logger
from src.analytics import compute_indicators
from src.db_writer import write_to_db

logger = get_logger(__name__)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

logger.info("Kafka Consumer started...")

for message in consumer:
    data = message.value
    logger.info(f"Consumed: {data}")
    enriched = compute_indicators(data)
    write_to_db(enriched)
