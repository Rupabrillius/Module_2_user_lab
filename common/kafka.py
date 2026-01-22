import json
import time
from kafka import KafkaProducer, KafkaConsumer

def make_producer(brokers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,
        retries=10,
    )

def make_consumer(brokers: str, topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=brokers.split(","),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000
    )

def safe_sleep(seconds: float) -> None:
    time.sleep(seconds)
