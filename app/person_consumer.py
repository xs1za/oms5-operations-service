from __future__ import annotations

import json
import logging
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition

from app.settings import settings
from app.storage import storage

logger = logging.getLogger(settings.service_name)

TOPIC = "people.performer.v1"
RETRY_DELAY_SECONDS = 5.0


def _log_gap(event: dict[str, Any]) -> None:
    logger.error(
        "Performer event version gap; offset is not committed until replay",
        extra={
            "event_id": event.get("event_id"),
            "aggregate_id": event.get("aggregate_id"),
            "aggregate_version": event.get("aggregate_version"),
            "error_code": "person_event_version_gap",
        },
    )


def apply_person_message(event: dict[str, Any]) -> str:
    """Apply one people.performer.v1 envelope. Returns applied/duplicate/stale/gap."""
    if not event.get("event_id") or event.get("aggregate_id") is None:
        logger.warning("Rejected malformed person event", extra={"event_id": event.get("event_id")})
        return "invalid"
    return storage.apply_person_event(event)


def run_person_consumer() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "oms5.people.performer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    logger.info("Started OMS5 Kafka consumer for %s", TOPIC)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", message.error())
                continue
            try:
                event = json.loads(message.value().decode("utf-8"))
            except json.JSONDecodeError:
                logger.exception("Rejected OMS5 people event: invalid JSON")
                consumer.commit(message=message, asynchronous=False)
                continue

            try:
                result = apply_person_message(event)
            except Exception:
                logger.exception(
                    "Failed to apply OMS5 people event; retrying without commit",
                    extra={"event_id": event.get("event_id")},
                )
                consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            if result == "gap":
                _log_gap(event)
                consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
