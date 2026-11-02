from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from confluent_kafka import Producer

from app.settings import settings
from app.storage import json_value, storage

logger = logging.getLogger(settings.service_name)

OUTBOX_TOPIC = "operations.performer.v1"
FACT_ASSIGNED = "operations.shift.assigned"
FACT_COMPLETED = "operations.shift.completed"
SCHEMA_VERSION = 1
MAX_LAST_ERROR_LENGTH = 500
PUBLISH_FLUSH_TIMEOUT_SECONDS = 10.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def funnel_outbox_enabled() -> bool:
    return str(settings.enable_funnel_outbox).strip().lower() not in {"0", "false", "no"}


def _producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 10000,
        }
    )


def build_envelope(
    event_type: str,
    person_id: int,
    payload: dict[str, Any],
    occurred_at: datetime,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": utc_iso(occurred_at),
        "producer": settings.service_name.lower(),
        "aggregate_type": "performer",
        "aggregate_id": str(person_id),
        "payload": payload,
    }


def enqueue_fact(
    connection,
    *,
    event_type: str,
    person_id: int,
    assignment_id: str | None,
    payload: dict[str, Any],
    occurred_at: datetime | None = None,
) -> dict[str, Any] | None:
    """Insert a funnel fact into operations_outbox using the caller's transaction."""
    if person_id is None:
        logger.warning(
            "Funnel fact skipped: person_id is missing",
            extra={"event_type": event_type, "assignment_id": assignment_id, "error_code": "funnel_fact_missing_person"},
        )
        return None
    occurred = occurred_at or utcnow()
    envelope = build_envelope(event_type, int(person_id), payload, occurred)
    row = connection.execute(
        """
        INSERT INTO operations_outbox
            (id, event_id, event_type, aggregate_type, aggregate_id, person_id,
             assignment_id, payload, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_type, assignment_id) WHERE assignment_id IS NOT NULL DO NOTHING
        RETURNING id
        """,
        (
            str(uuid4()), envelope["event_id"], event_type, envelope["aggregate_type"],
            envelope["aggregate_id"], int(person_id), assignment_id, json_value(envelope), occurred,
        ),
    ).fetchone()
    if row is None:
        logger.info(
            "Funnel fact already enqueued",
            extra={
                "event_id": envelope["event_id"],
                "event_type": event_type,
                "assignment_id": assignment_id,
                "person_id": int(person_id),
                "error_code": "funnel_fact_duplicate",
            },
        )
        return None
    logger.info(
        "Funnel fact enqueued",
        extra={
            "event_id": envelope["event_id"],
            "event_type": event_type,
            "assignment_id": assignment_id,
            "person_id": int(person_id),
            "shift_id": payload.get("shift_id"),
        },
    )
    return envelope


def _produce_row(producer: Producer, row: dict[str, Any], on_delivery) -> None:
    producer.produce(
        OUTBOX_TOPIC,
        key=str(row["person_id"]).encode("utf-8"),
        value=json.dumps(row["payload"], default=str).encode("utf-8"),
        on_delivery=on_delivery,
    )


def dispatch_outbox_once(batch_size: int | None = None, producer_factory: Callable[[], Any] | None = None) -> int:
    """Publish one batch of pending outbox rows. Returns the number of published rows."""
    limit = int(batch_size or settings.funnel_outbox_batch_size or 100)
    factory = producer_factory or _producer
    published = 0
    with storage.connection() as connection:
        rows = connection.execute(
            """
            SELECT id, event_id, event_type, person_id, assignment_id, payload, attempts
            FROM operations_outbox
            WHERE published_at IS NULL
            ORDER BY created_at, id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (limit,),
        ).fetchall()
        if not rows:
            return 0

        results: dict[str, Exception | None] = {}
        producer = None
        producer_error: Exception | None = None
        try:
            producer = factory()
        except Exception as exc:
            producer_error = exc
        if producer is not None:
            def callback_for(row_id: str):
                def on_delivery(error, _message) -> None:
                    results[row_id] = RuntimeError(str(error)) if error is not None else None
                return on_delivery

            produced = 0
            for row in rows:
                row_id = str(row["id"])
                try:
                    _produce_row(producer, row, callback_for(row_id))
                    produced += 1
                except Exception as exc:
                    results[row_id] = exc
            if produced:
                producer.flush(PUBLISH_FLUSH_TIMEOUT_SECONDS)

        for row in rows:
            row_id = str(row["id"])
            info = {
                "event_id": str(row["event_id"]),
                "event_type": row["event_type"],
                "assignment_id": str(row["assignment_id"]) if row["assignment_id"] else None,
                "person_id": row["person_id"],
                "shift_id": (row["payload"] or {}).get("payload", {}).get("shift_id"),
                "attempts": row["attempts"],
            }
            error = producer_error
            if error is None:
                if row_id in results:
                    error = results[row_id]
                else:
                    error = RuntimeError("Kafka did not confirm message")
            if error is not None:
                error_text = str(error)[:MAX_LAST_ERROR_LENGTH] or error.__class__.__name__
                connection.execute(
                    """
                    UPDATE operations_outbox
                    SET attempts = attempts + 1, last_error = %s
                    WHERE id = %s
                    """,
                    (error_text, row["id"]),
                )
                logger.warning(
                    "Funnel outbox publish failed",
                    extra={**info, "attempts": row["attempts"] + 1, "error": error_text, "error_code": "funnel_outbox_publish_failed"},
                )
                continue
            connection.execute(
                "UPDATE operations_outbox SET published_at = %s WHERE id = %s",
                (utcnow(), row["id"]),
            )
            published += 1
            logger.info("Funnel outbox fact published", extra={**info, "error_code": None})
    return published


def run_funnel_outbox(poll_interval: float | None = None) -> None:
    interval = settings.funnel_outbox_poll_interval if poll_interval is None else poll_interval
    logger.info("Started funnel outbox publisher", extra={"topic": OUTBOX_TOPIC, "poll_interval": interval})
    while True:
        try:
            dispatch_outbox_once()
        except Exception:
            logger.exception("Funnel outbox dispatch failed", extra={"error_code": "funnel_outbox_dispatch_failed"})
        time.sleep(interval)
