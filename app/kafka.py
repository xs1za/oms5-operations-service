import json
import logging
from typing import Any

from confluent_kafka import Producer

from app.settings import settings

logger = logging.getLogger(__name__)


def publish_event(topic: str, payload: dict[str, Any], key: str | None = None) -> None:
    try:
        producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        producer.produce(topic, key=key, value=json.dumps(payload, default=str).encode("utf-8"))
        producer.flush(2)
        logger.info("Published Kafka event to %s", topic, extra={"kafka_key": key})
    except Exception:
        logger.exception("Failed to publish Kafka event to %s", topic)
