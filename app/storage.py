from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.settings import settings


MIGRATIONS_DIR = Path(__file__).with_name("migrations")
MIGRATION_PATH = MIGRATIONS_DIR / "001_create_operational_state.sql"

logger = logging.getLogger(settings.service_name)

ENGAGEMENT_TYPES = {"gph", "self_employed"}
ENGAGEMENT_VALIDATION_STATUSES = {"pending", "verified", "rejected"}
MEDICAL_BOOK_STATUSES = {"none", "non_food", "food"}
FUNNEL_STAGES = {
    "PERFORMER_BLANK",
    "PERFORMER_PROFILE",
    "PERFORMER_DOCS_PENDING",
    "PERFORMER_READY_NON_MEDICAL",
    "PERFORMER_READY_NON_FOOD",
    "PERFORMER_READY_FOOD",
    "PERFORMER_BOOKED_SHIFT",
    "PERFORMER_FIRST_SHIFT_COMPLETED",
    "PERFORMER_FIFTH_SHIFT_COMPLETED",
    "PERFORMER_PROFILE_DELETED",
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def allowed_value(value: Any, allowed: set[str], field: str, person_id: int | None = None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in allowed:
        logger.warning(
            "Ignored unknown %s value",
            field,
            extra={"person_id": person_id, "value": value, "error_code": f"unknown_{field}"},
        )
        return None
    return value


def keep_or_update(payload: dict[str, Any], key: str, current: str | None, allowed: set[str], person_id: int) -> str | None:
    value = payload.get(key)
    if value is None:
        return current
    return allowed_value(value, allowed, key, person_id) or current


def max_completed_shifts(current: Any, value: Any) -> int:
    try:
        incoming = int(value)
    except (TypeError, ValueError):
        return int(current or 0)
    return max(int(current or 0), incoming)


def json_value(value: Any) -> Jsonb:
    return Jsonb(json.loads(json.dumps(value, default=str)))


def normalize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    if isinstance(result.get("status"), str):
        result["status"] = result["status"]
    return result


class OperationalStorage:
    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(settings.database_url, row_factory=dict_row) as connection:
            yield connection

    def migrate(self) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(100) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            applied = {
                row["version"]
                for row in connection.execute("SELECT version FROM schema_migrations").fetchall()
            }
            for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                if path.stem in applied:
                    continue
                connection.execute(path.read_text(encoding="utf-8"))
                connection.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s) ON CONFLICT (version) DO NOTHING",
                    (path.stem,),
                )
                logger.info("Applied migration %s", path.stem)

    def check(self) -> dict[str, Any]:
        try:
            with self.connection() as connection:
                connection.execute("SELECT 1")
            return {"status": "ok"}
        except Exception as exc:
            return {"status": "failed", "error": str(exc)}

    def create_client(self, item: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as connection:
            row = connection.execute(
                """
                INSERT INTO clients
                    (id, name, external_id, client_type_code, parent_client_id, created_at,
                     address, country_code, timezone, lon, lat)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    item["id"], item["name"], item.get("external_id"), item.get("client_type_code", "object"),
                    item.get("parent_client_id"), item["created_at"], item.get("address"),
                    item.get("country_code"), item.get("timezone"), item.get("lon"), item.get("lat"),
                ),
            ).fetchone()
            return normalize_row(row)  # type: ignore[return-value]

    def get_client(self, client_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            return normalize_row(connection.execute("SELECT * FROM clients WHERE id = %s", (client_id,)).fetchone())

    def client_within_scope(self, organization_id: str, client_id: str) -> bool:
        current: str | None = client_id
        while current is not None:
            if current == organization_id:
                return True
            row = self.get_client(current)
            if row is None:
                return False
            current = row.get("parent_client_id")
        return False

    def client_depth(self, client_id: str) -> int:
        depth = 0
        current: str | None = client_id
        while current is not None:
            row = self.get_client(current)
            if row is None:
                break
            current = row.get("parent_client_id")
            depth += 1
        return depth

    def node_has_children(self, client_id: str) -> bool:
        with self.connection() as connection:
            count = connection.execute(
                "SELECT count(*) AS count FROM clients WHERE parent_client_id = %s", (client_id,)
            ).fetchone()["count"]
            return count > 0

    def descendant_client_ids(self, organization_id: str) -> list[str]:
        with self.connection() as connection:
            rows = connection.execute("SELECT id, parent_client_id FROM clients").fetchall()
        children: dict[str, list[str]] = {}
        for row in rows:
            children.setdefault(row["parent_client_id"], []).append(row["id"])
        result: list[str] = []
        stack = [organization_id]
        while stack:
            node = stack.pop()
            result.append(node)
            stack.extend(children.get(node, []))
        return result

    def assignment_stats(self, organization_id: str) -> dict[str, Any]:
        client_ids = self.descendant_client_ids(organization_id)
        if not client_ids:
            return {"organization_id": organization_id, "total_shifts": 0, "assigned_shifts": 0, "unassigned_shifts": 0}
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT assigned_performer_id
                FROM shifts
                WHERE client_id = ANY(%s)
                """,
                (client_ids,),
            ).fetchall()
        total = len(rows)
        assigned = sum(1 for row in rows if row["assigned_performer_id"] is not None)
        return {
            "organization_id": organization_id,
            "total_shifts": total,
            "assigned_shifts": assigned,
            "unassigned_shifts": total - assigned,
        }

    def sync_performer(self, item: dict[str, Any], created: bool) -> dict[str, Any]:
        with self.connection() as connection:
            if created:
                row = connection.execute(
                    """
                    INSERT INTO performers
                        (id, person_id, display_name, email, phone, status, source_version, synced_at,
                         engagement_type, engagement_type_validation_status, funnel_stage, medical_book_status,
                         completed_shifts_count, first_booked_at, first_shift_completed_at, fifth_shift_completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        item["id"], item["person_id"], item["display_name"], item["email"],
                        item["phone"], item["status"], item["source_version"], item["synced_at"],
                        self._engagement_type(item.get("engagement_type"), item["person_id"]),
                        self._validation_status(item.get("engagement_type_validation_status"), item["person_id"]),
                        self._funnel_stage(item.get("funnel_stage"), item["person_id"]),
                        allowed_value(item.get("medical_book_status"), MEDICAL_BOOK_STATUSES, "medical_book_status", item["person_id"]),
                        max_completed_shifts(0, item.get("completed_shifts_count")),
                        parse_datetime(item.get("first_booked_at")),
                        parse_datetime(item.get("first_shift_completed_at")),
                        parse_datetime(item.get("fifth_shift_completed_at")),
                    ),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    UPDATE performers
                    SET display_name = %s, email = %s, phone = %s, status = %s,
                        source_version = %s, synced_at = %s,
                        engagement_type = COALESCE(%s, engagement_type),
                        engagement_type_validation_status = COALESCE(%s, engagement_type_validation_status),
                        funnel_stage = COALESCE(%s, funnel_stage),
                        medical_book_status = COALESCE(%s, medical_book_status),
                        completed_shifts_count = GREATEST(completed_shifts_count, %s),
                        first_booked_at = COALESCE(%s, first_booked_at),
                        first_shift_completed_at = COALESCE(%s, first_shift_completed_at),
                        fifth_shift_completed_at = COALESCE(%s, fifth_shift_completed_at)
                    WHERE person_id = %s AND merged_into_performer_id IS NULL
                    RETURNING *
                    """,
                    (
                        item["display_name"], item["email"], item["phone"], item["status"],
                        item["source_version"], item["synced_at"],
                        self._engagement_type(item.get("engagement_type"), item["person_id"]),
                        self._validation_status(item.get("engagement_type_validation_status"), item["person_id"]),
                        self._funnel_stage(item.get("funnel_stage"), item["person_id"]),
                        allowed_value(item.get("medical_book_status"), MEDICAL_BOOK_STATUSES, "medical_book_status", item["person_id"]),
                        max_completed_shifts(0, item.get("completed_shifts_count")),
                        parse_datetime(item.get("first_booked_at")),
                        parse_datetime(item.get("first_shift_completed_at")),
                        parse_datetime(item.get("fifth_shift_completed_at")),
                        item["person_id"],
                    ),
                ).fetchone()
            return normalize_row(row)  # type: ignore[return-value]

    @staticmethod
    def _engagement_type(value: Any, person_id: int | None) -> str | None:
        return allowed_value(value, ENGAGEMENT_TYPES, "engagement_type", person_id)

    @staticmethod
    def _validation_status(value: Any, person_id: int | None) -> str | None:
        return allowed_value(value, ENGAGEMENT_VALIDATION_STATUSES, "engagement_type_validation_status", person_id)

    @staticmethod
    def _funnel_stage(value: Any, person_id: int | None) -> str | None:
        return allowed_value(value, FUNNEL_STAGES, "funnel_stage", person_id)

    def get_performer_by_person_id(self, person_id: int) -> dict[str, Any] | None:
        with self.connection() as connection:
            return normalize_row(
                connection.execute(
                    "SELECT * FROM performers WHERE person_id = %s AND merged_into_performer_id IS NULL",
                    (person_id,),
                ).fetchone()
            )

    def get_performer(self, performer_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            return normalize_row(connection.execute("SELECT * FROM performers WHERE id = %s", (performer_id,)).fetchone())

    def update_performer(self, performer_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        if not values:
            return self.get_performer(performer_id)
        assignments = ", ".join(f"{column} = %s" for column in values)
        params = list(values.values()) + [performer_id]
        with self.connection() as connection:
            row = connection.execute(
                f"UPDATE performers SET {assignments}, synced_at = now() WHERE id = %s RETURNING *",
                params,
            ).fetchone()
            return normalize_row(row) if row else None

    def apply_person_event(self, event: dict[str, Any]) -> str:
        """Apply a people.performer.v1 event transactionally, with inbox and version ordering."""
        event_id = event.get("event_id")
        aggregate_id = int(event.get("aggregate_id"))
        aggregate_version = int(event.get("aggregate_version") or 0)
        event_type = event.get("event_type") or ""
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        now = utcnow()

        with self.connection() as connection:
            duplicate = connection.execute(
                "SELECT event_id FROM person_event_inbox WHERE event_id = %s", (event_id,)
            ).fetchone()
            if duplicate is not None:
                return "duplicate"

            state = connection.execute(
                "SELECT last_version FROM person_consumer_state WHERE aggregate_id = %s FOR UPDATE",
                (aggregate_id,),
            ).fetchone()
            last_version = int(state["last_version"]) if state else 0
            if aggregate_version <= last_version:
                return "stale"
            if aggregate_version != last_version + 1:
                return "gap"

            if event_type == "people.merged":
                self._apply_merged_event(connection, payload, aggregate_id, now)
            else:
                self._apply_performer_profile(connection, payload, aggregate_id, now, aggregate_version)

            connection.execute(
                """
                INSERT INTO person_event_inbox
                    (event_id, event_type, aggregate_id, aggregate_version, payload, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (event_id, event_type, aggregate_id, aggregate_version, json_value(payload), now),
            )
            connection.execute(
                """
                INSERT INTO person_consumer_state (aggregate_id, last_version, last_event_id, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (aggregate_id) DO UPDATE
                SET last_version = EXCLUDED.last_version,
                    last_event_id = EXCLUDED.last_event_id,
                    updated_at = EXCLUDED.updated_at
                """,
                (aggregate_id, aggregate_version, event_id, now),
            )
            return "applied"

    def _apply_performer_profile(self, connection, payload: dict[str, Any], person_id: int, now: datetime, aggregate_version: int) -> None:
        row = connection.execute(
            "SELECT * FROM performers WHERE person_id = %s AND merged_into_performer_id IS NULL",
            (person_id,),
        ).fetchone()
        display_name = (payload.get("display_name") or f"Performer {person_id}")[:255]
        status = payload.get("profile_status") or payload.get("status") or "active"
        if row is None:
            connection.execute(
                """
                INSERT INTO performers
                    (id, person_id, display_name, email, phone, status, source_version, synced_at,
                     engagement_type, engagement_type_validation_status, funnel_stage, medical_book_status,
                     completed_shifts_count, first_booked_at, first_shift_completed_at, fifth_shift_completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(__import__("uuid").uuid4()), person_id, display_name, payload.get("email"),
                    payload.get("phone"), status, max(aggregate_version, 1), now,
                    self._engagement_type(payload.get("engagement_type"), person_id),
                    self._validation_status(payload.get("engagement_type_validation_status"), person_id),
                    self._funnel_stage(payload.get("funnel_stage"), person_id),
                    allowed_value(payload.get("medical_book_status"), MEDICAL_BOOK_STATUSES, "medical_book_status", person_id),
                    max_completed_shifts(0, payload.get("completed_shifts_count")),
                    parse_datetime(payload.get("first_booked_at")),
                    parse_datetime(payload.get("first_shift_completed_at")),
                    parse_datetime(payload.get("fifth_shift_completed_at")),
                ),
            )
            return
        connection.execute(
            """
            UPDATE performers
            SET display_name = %s, email = %s, phone = %s, status = %s, source_version = %s, synced_at = %s,
                engagement_type = %s, engagement_type_validation_status = %s, funnel_stage = %s,
                medical_book_status = %s, completed_shifts_count = %s, first_booked_at = %s,
                first_shift_completed_at = %s, fifth_shift_completed_at = %s
            WHERE id = %s
            """,
            (
                display_name, payload.get("email"), payload.get("phone"), status,
                max(aggregate_version, 1), now,
                keep_or_update(payload, "engagement_type", row["engagement_type"], ENGAGEMENT_TYPES, person_id),
                keep_or_update(payload, "engagement_type_validation_status", row["engagement_type_validation_status"], ENGAGEMENT_VALIDATION_STATUSES, person_id),
                keep_or_update(payload, "funnel_stage", row["funnel_stage"], FUNNEL_STAGES, person_id),
                keep_or_update(payload, "medical_book_status", row["medical_book_status"], MEDICAL_BOOK_STATUSES, person_id),
                max_completed_shifts(row["completed_shifts_count"], payload.get("completed_shifts_count")),
                parse_datetime(payload.get("first_booked_at")) or row["first_booked_at"],
                parse_datetime(payload.get("first_shift_completed_at")) or row["first_shift_completed_at"],
                parse_datetime(payload.get("fifth_shift_completed_at")) or row["fifth_shift_completed_at"],
                row["id"],
            ),
        )

    def _apply_merged_event(self, connection, payload: dict[str, Any], duplicate_person_id: int, now: datetime) -> None:
        canonical_person_id = payload.get("canonical_person_id")
        if canonical_person_id is None:
            return
        canonical_person_id = int(canonical_person_id)

        duplicate = connection.execute(
            "SELECT * FROM performers WHERE person_id = %s AND merged_into_performer_id IS NULL",
            (duplicate_person_id,),
        ).fetchone()
        canonical = connection.execute(
            "SELECT * FROM performers WHERE person_id = %s AND merged_into_performer_id IS NULL",
            (canonical_person_id,),
        ).fetchone()

        if duplicate is None:
            return
        if canonical is None:
            # Canonical profile not received yet: keep the profile id and rebind it to the canonical person.
            connection.execute(
                "UPDATE performers SET person_id = %s, synced_at = %s WHERE id = %s",
                (canonical_person_id, now, duplicate["id"]),
            )
            return
        connection.execute(
            "UPDATE performers SET status = 'merged', merged_into_performer_id = %s, synced_at = %s WHERE id = %s",
            (canonical["id"], now, duplicate["id"]),
        )

    def create_shift(self, item: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as connection:
            row = connection.execute(
                """
                INSERT INTO shifts (
                    id, client_id, starts_at, ends_at, location, required_performers_count,
                    created_by_user_id, created_by_person_id, created_by_display_name,
                    status, assigned_performer_id, assigned_performer_person_id, close_reason,
                    failure_reason, auto_closed, auto_close_reason, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    item["id"], item["client_id"], item["starts_at"], item["ends_at"], item["location"],
                    item["required_performers_count"], item.get("created_by_user_id"), item.get("created_by_person_id"),
                    item.get("created_by_display_name"), item["status"], item["assigned_performer_id"],
                    item.get("assigned_performer_person_id"), item["close_reason"], item["failure_reason"],
                    item["auto_closed"], item["auto_close_reason"], item["created_at"], item["updated_at"],
                ),
            ).fetchone()
            return normalize_row(row)  # type: ignore[return-value]

    def get_shift(self, shift_id: str, for_update: bool = False, connection=None) -> dict[str, Any] | None:
        query = "SELECT * FROM shifts WHERE id = %s"
        if for_update:
            query += " FOR UPDATE"
        if connection is not None:
            return normalize_row(connection.execute(query, (shift_id,)).fetchone())
        with self.connection() as db:
            return normalize_row(db.execute(query, (shift_id,)).fetchone())

    def update_shift(self, connection, shift: dict[str, Any]) -> dict[str, Any]:
        row = connection.execute(
            """
            UPDATE shifts
            SET status = %s, assigned_performer_id = %s, assigned_performer_person_id = %s, close_reason = %s,
                failure_reason = %s, auto_closed = %s, auto_close_reason = %s, updated_at = %s
            WHERE id = %s
            RETURNING *
            """,
            (
                shift["status"], shift["assigned_performer_id"], shift.get("assigned_performer_person_id"),
                shift["close_reason"], shift["failure_reason"], shift["auto_closed"], shift["auto_close_reason"],
                shift["updated_at"], shift["id"],
            ),
        ).fetchone()
        return normalize_row(row)  # type: ignore[return-value]

    def update_shift_fields(self, shift_id: str, values: dict[str, Any]) -> dict[str, Any] | None:
        if not values:
            return self.get_shift(shift_id)
        assignments = ", ".join(f"{column} = %s" for column in values)
        params = list(values.values()) + [shift_id]
        with self.connection() as connection:
            row = connection.execute(
                f"UPDATE shifts SET {assignments}, updated_at = now() WHERE id = %s RETURNING *",
                params,
            ).fetchone()
            return normalize_row(row) if row else None

    def insert_history(self, connection, item: dict[str, Any]) -> dict[str, Any]:
        row = connection.execute(
            """
            INSERT INTO shift_status_history
                (id, shift_id, from_status, to_status, transition_code, reason,
                 actor_user_id, actor_person_id, actor_display_name, occurred_at, correlation_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                item["id"], item["shift_id"], item["from_status"], item["to_status"],
                item["transition_code"], item["reason"], item["actor_user_id"], item.get("actor_person_id"),
                item.get("actor_display_name"), item["occurred_at"], item["correlation_id"], json_value(item["metadata"]),
            ),
        ).fetchone()
        return normalize_row(row)  # type: ignore[return-value]

    def get_history(self, shift_id: str) -> list[dict[str, Any]]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM shift_status_history WHERE shift_id = %s ORDER BY occurred_at, id",
                (shift_id,),
            ).fetchall()
            return [normalize_row(row) for row in rows]  # type: ignore[misc]

    def create_campaign(self, campaign: dict[str, Any], offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self.connection() as connection:
            connection.execute(
                """
                INSERT INTO offer_campaigns (id, shift_id, status, created_by_user_id, created_by_person_id, created_by_display_name, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    campaign["id"], campaign["shift_id"], campaign["status"], campaign.get("created_by_user_id"),
                    campaign.get("created_by_person_id"), campaign.get("created_by_display_name"), campaign["created_at"],
                ),
            )
            result = []
            for offer in offers:
                row = connection.execute(
                    """
                    INSERT INTO shift_offers
                        (id, campaign_id, shift_id, performer_id, status, token_hash,
                         expires_at, sent_at, responded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        offer["id"], offer["campaign_id"], offer["shift_id"], offer["performer_id"],
                        offer["status"], offer["token_hash"], offer["expires_at"], offer["sent_at"], offer["responded_at"],
                    ),
                ).fetchone()
                result.append(normalize_row(row))
            return result  # type: ignore[return-value]

    def get_offer_by_token_hash(self, token_hash: str, connection=None, for_update: bool = False) -> dict[str, Any] | None:
        query = "SELECT * FROM shift_offers WHERE token_hash = %s"
        if for_update:
            query += " FOR UPDATE"
        if connection is not None:
            return normalize_row(connection.execute(query, (token_hash,)).fetchone())
        with self.connection() as db:
            return normalize_row(db.execute(query, (token_hash,)).fetchone())

    def get_offer_context(self, token_hash: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        with self.connection() as connection:
            offer = connection.execute("SELECT * FROM shift_offers WHERE token_hash = %s", (token_hash,)).fetchone()
            if offer is None:
                return None
            shift = connection.execute("SELECT * FROM shifts WHERE id = %s", (offer["shift_id"],)).fetchone()
            performer = connection.execute("SELECT * FROM performers WHERE id = %s", (offer["performer_id"],)).fetchone()
            if shift is None or performer is None:
                return None
            return normalize_row(offer), normalize_row(shift), normalize_row(performer)  # type: ignore[return-value]

    def mark_offer_viewed(self, token_hash: str, view: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        with self.connection() as connection:
            offer = self.get_offer_by_token_hash(token_hash, connection=connection, for_update=True)
            if offer is None:
                raise KeyError("offer_token_not_found")
            connection.execute(
                """
                INSERT INTO shift_offer_views (id, offer_id, viewed_at, ip_address, user_agent, correlation_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (view["id"], offer["id"], view["viewed_at"], view["ip_address"], view["user_agent"], view["correlation_id"]),
            )
            if offer["status"] == "sent":
                offer["status"] = "viewed"
                offer["responded_at"] = offer["responded_at"]
                offer = normalize_row(connection.execute(
                    "UPDATE shift_offers SET status = %s WHERE id = %s RETURNING *",
                    (offer["status"], offer["id"]),
                ).fetchone())
            shift = self.get_shift(offer["shift_id"], connection=connection)
            return offer, shift  # type: ignore[return-value]

    def accept_offer(self, token_hash: str, assignment: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as connection:
            offer = self.get_offer_by_token_hash(token_hash, connection=connection)
            if offer is None:
                raise KeyError("offer_token_not_found")
            shift = self.get_shift(offer["shift_id"], for_update=True, connection=connection)
            offer = self.get_offer_by_token_hash(token_hash, connection=connection, for_update=True)
            if offer is None or shift is None:
                raise KeyError("offer_token_not_found")
            if shift.get("assigned_performer_id"):
                connection.execute("UPDATE shift_offers SET status = 'lost' WHERE id = %s", (offer["id"],))
                return {"conflict": True, "offer": offer, "shift": shift}
            if offer["status"] in {"lost", "expired", "cancelled", "declined"}:
                return {"conflict": True, "reason": f"offer_{offer['status']}", "offer": offer, "shift": shift}
            if datetime.now(timezone.utc) >= offer["expires_at"].astimezone(timezone.utc):
                return {"conflict": True, "reason": "token_expired", "offer": offer, "shift": shift}
            if shift["status"] != "A20_SOURCING":
                return {"conflict": True, "reason": "shift_not_sourcing", "offer": offer, "shift": shift}

            performer = connection.execute(
                "SELECT id, person_id, engagement_type FROM performers WHERE id = %s",
                (offer["performer_id"],),
            ).fetchone()
            if performer is None or not performer["engagement_type"]:
                return {"conflict": True, "reason": "performer_not_ready", "offer": offer, "shift": shift}

            assignment["engagement_type"] = performer["engagement_type"]
            offer = normalize_row(connection.execute(
                """
                UPDATE shift_offers SET status = 'won', responded_at = %s
                WHERE id = %s RETURNING *
                """,
                (assignment["assigned_at"], offer["id"]),
            ).fetchone())
            connection.execute(
                """
                INSERT INTO shift_assignments
                    (id, shift_id, performer_id, offer_id, status, assigned_at, removed_at, reason, engagement_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    assignment["id"], assignment["shift_id"], assignment["performer_id"], assignment["offer_id"],
                    assignment["status"], assignment["assigned_at"], assignment["removed_at"], assignment["reason"],
                    assignment["engagement_type"],
                ),
            )
            lost_offers = connection.execute(
                """
                UPDATE shift_offers SET status = 'lost'
                WHERE shift_id = %s AND id <> %s AND status IN ('sent', 'viewed')
                RETURNING *
                """,
                (shift["id"], offer["id"]),
            ).fetchall()
            shift["assigned_performer_id"] = offer["performer_id"]
            shift["assigned_performer_person_id"] = performer["person_id"]
            shift["status"] = "A30_CHOICE"
            shift["updated_at"] = assignment["assigned_at"]
            shift = self.update_shift(connection, shift)
            history = self.insert_history(connection, history)
            self._enqueue_assignment_fact(connection, shift, assignment, performer)
            return {
                "conflict": False,
                "offer": offer,
                "assignment": assignment,
                "shift": shift,
                "history": history,
                "lost_offers": [normalize_row(row) for row in lost_offers],
            }

    def _enqueue_assignment_fact(self, connection, shift: dict[str, Any], assignment: dict[str, Any], performer: dict[str, Any]) -> None:
        from app.funnel_outbox import FACT_ASSIGNED, enqueue_fact

        person_id = shift.get("assigned_performer_person_id") or performer.get("person_id")
        if person_id is None:
            logger.warning(
                "Funnel assignment fact skipped: person_id is missing",
                extra={
                    "assignment_id": assignment.get("id"),
                    "shift_id": shift.get("id"),
                    "error_code": "funnel_fact_missing_person",
                },
            )
            return
        enqueue_fact(
            connection,
            event_type=FACT_ASSIGNED,
            person_id=int(person_id),
            assignment_id=assignment["id"],
            occurred_at=assignment["assigned_at"],
            payload={
                "person_id": str(person_id),
                "performer_id": assignment["performer_id"],
                "shift_id": assignment["shift_id"],
                "assignment_id": assignment["id"],
                "engagement_type": assignment.get("engagement_type"),
                "assigned_at": utc_iso(assignment["assigned_at"]),
            },
        )

    def prepare_shift_completion(self, connection, shift: dict[str, Any], occurred_at: datetime) -> dict[str, Any] | None:
        """Fill assignment completion and return funnel fact arguments for a finished shift."""
        from app.funnel_outbox import FACT_COMPLETED

        if not shift.get("assigned_performer_id"):
            return None
        assignment = connection.execute(
            """
            SELECT * FROM shift_assignments
            WHERE shift_id = %s AND status = 'assigned' AND removed_at IS NULL AND completed_at IS NULL
            ORDER BY assigned_at DESC, id
            LIMIT 1
            FOR UPDATE
            """,
            (shift["id"],),
        ).fetchone()
        if assignment is None:
            return None
        connection.execute(
            "UPDATE shift_assignments SET completed_at = %s WHERE id = %s",
            (occurred_at, assignment["id"]),
        )
        person_id = shift.get("assigned_performer_person_id")
        if person_id is None:
            performer = connection.execute(
                "SELECT person_id FROM performers WHERE id = %s",
                (assignment["performer_id"],),
            ).fetchone()
            person_id = performer["person_id"] if performer else None
        if person_id is None:
            logger.warning(
                "Funnel completion fact skipped: person_id is missing",
                extra={
                    "assignment_id": assignment["id"],
                    "shift_id": shift["id"],
                    "error_code": "funnel_fact_missing_person",
                },
            )
            return None
        timesheets = connection.execute(
            """
            SELECT id, performer_id, work_date, hours
            FROM timesheets
            WHERE shift_id = %s AND performer_id = %s
            ORDER BY work_date, id
            """,
            (shift["id"], assignment["performer_id"]),
        ).fetchall()
        total_hours = sum(float(row["hours"]) for row in timesheets)
        return {
            "event_type": FACT_COMPLETED,
            "person_id": int(person_id),
            "assignment_id": assignment["id"],
            "occurred_at": occurred_at,
            "payload": {
                "person_id": str(person_id),
                "performer_id": assignment["performer_id"],
                "shift_id": assignment["shift_id"],
                "assignment_id": assignment["id"],
                "engagement_type": assignment["engagement_type"],
                "completed_at": utc_iso(occurred_at),
                "hours": total_hours,
                "timesheets": [
                    {
                        "id": row["id"],
                        "performer_id": row["performer_id"],
                        "work_date": row["work_date"].isoformat(),
                        "hours": float(row["hours"]),
                    }
                    for row in timesheets
                ],
            },
        }

    def decline_offer(self, token_hash: str) -> tuple[dict[str, Any], dict[str, Any], bool]:
        with self.connection() as connection:
            offer = self.get_offer_by_token_hash(token_hash, connection=connection, for_update=True)
            if offer is None:
                raise KeyError("offer_token_not_found")
            changed = offer["status"] not in {"won", "lost", "expired", "cancelled"}
            if changed:
                offer = normalize_row(connection.execute(
                    "UPDATE shift_offers SET status = 'declined', responded_at = %s WHERE id = %s RETURNING *",
                    (utcnow(), offer["id"]),
                ).fetchone())
            shift = self.get_shift(offer["shift_id"], connection=connection)
            return offer, shift, changed  # type: ignore[return-value]

    def create_timesheet(self, item: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as connection:
            row = connection.execute(
                """
                INSERT INTO timesheets (id, performer_id, shift_id, work_date, hours, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (item["id"], item["performer_id"], item["shift_id"], item["work_date"], item["hours"], item["status"], item["created_at"]),
            ).fetchone()
            return normalize_row(row)  # type: ignore[return-value]

    def list_shifts(self, starts_at_from: datetime, starts_at_to: datetime) -> list[dict[str, Any]]:
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT id, client_id, starts_at, ends_at, status, location,
                       assigned_performer_id, close_reason, failure_reason, created_at, updated_at
                FROM shifts
                WHERE starts_at >= %s AND starts_at <= %s
                ORDER BY starts_at, id
                """,
                (starts_at_from, starts_at_to),
            ).fetchall()
            return [normalize_row(row) for row in rows]  # type: ignore[misc]

    def auto_close_unfilled(self, close_at: datetime) -> tuple[list[str], list[dict[str, Any]]]:
        closed: list[str] = []
        events: list[dict[str, Any]] = []
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM shifts
                WHERE status = 'A20_SOURCING' AND assigned_performer_id IS NULL
                FOR UPDATE
                """,
            ).fetchall()
            for shift in rows:
                shift = normalize_row(shift)
                if close_at <= shift["starts_at"]:
                    continue
                reporting_close = datetime(
                    shift["starts_at"].year + (1 if shift["starts_at"].month == 12 else 0),
                    1 if shift["starts_at"].month == 12 else shift["starts_at"].month + 1,
                    10,
                    23,
                    59,
                    59,
                    999999,
                    tzinfo=timezone.utc,
                )
                if close_at <= reporting_close:
                    continue
                now = utcnow()
                shift.update({
                    "status": "A80_CLOSED", "close_reason": "failed", "failure_reason": "no_performer_found",
                    "auto_closed": True, "auto_close_reason": "reporting_period_expired", "updated_at": now,
                })
                history = {
                    "id": str(__import__("uuid").uuid4()), "shift_id": shift["id"], "from_status": "A20_SOURCING",
                    "to_status": "A80_CLOSED", "transition_code": "A20_SOURCING->A80_CLOSED",
                    "reason": "reporting_period_expired", "actor_user_id": None, "occurred_at": now,
                    "correlation_id": str(__import__("uuid").uuid4()), "metadata": {},
                }
                self.update_shift(connection, shift)
                self.insert_history(connection, history)
                closed.append(shift["id"])
                events.append({"shift": shift, "history": history})
        return closed, events

    def summary(self) -> dict[str, int]:
        with self.connection() as connection:
            result = {}
            for table, key in (
                ("clients", "clients"), ("performers", "performers"), ("shifts", "shifts"),
                ("offer_campaigns", "offerCampaigns"), ("shift_offers", "offers"),
                ("shift_assignments", "assignments"), ("timesheets", "timesheets"),
            ):
                result[key] = connection.execute(f"SELECT count(*) AS count FROM {table}").fetchone()["count"]
            return result


storage = OperationalStorage()
