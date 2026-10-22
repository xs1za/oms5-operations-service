from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.settings import settings


MIGRATION_PATH = Path(__file__).with_name("migrations") / "001_create_operational_state.sql"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
            connection.execute(MIGRATION_PATH.read_text(encoding="utf-8"))

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
                INSERT INTO clients (id, name, external_id, created_at)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (item["id"], item["name"], item["external_id"], item["created_at"]),
            ).fetchone()
            return normalize_row(row)  # type: ignore[return-value]

    def get_client(self, client_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            return normalize_row(connection.execute("SELECT * FROM clients WHERE id = %s", (client_id,)).fetchone())

    def sync_performer(self, item: dict[str, Any], created: bool) -> dict[str, Any]:
        with self.connection() as connection:
            if created:
                row = connection.execute(
                    """
                    INSERT INTO performers
                        (id, employee_id, display_name, email, phone, status, source_version, synced_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        item["id"], item["employee_id"], item["display_name"], item["email"],
                        item["phone"], item["status"], item["source_version"], item["synced_at"],
                    ),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    UPDATE performers
                    SET display_name = %s, email = %s, phone = %s, status = %s,
                        source_version = %s, synced_at = %s
                    WHERE employee_id = %s
                    RETURNING *
                    """,
                    (
                        item["display_name"], item["email"], item["phone"], item["status"],
                        item["source_version"], item["synced_at"], item["employee_id"],
                    ),
                ).fetchone()
            return normalize_row(row)  # type: ignore[return-value]

    def get_performer_by_employee_id(self, employee_id: int) -> dict[str, Any] | None:
        with self.connection() as connection:
            return normalize_row(
                connection.execute("SELECT * FROM performers WHERE employee_id = %s", (employee_id,)).fetchone()
            )

    def get_performer(self, performer_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            return normalize_row(connection.execute("SELECT * FROM performers WHERE id = %s", (performer_id,)).fetchone())

    def create_shift(self, item: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as connection:
            row = connection.execute(
                """
                INSERT INTO shifts (
                    id, client_id, starts_at, ends_at, location, required_performers_count,
                    created_by_user_id, status, assigned_performer_id, close_reason,
                    failure_reason, auto_closed, auto_close_reason, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    item["id"], item["client_id"], item["starts_at"], item["ends_at"], item["location"],
                    item["required_performers_count"], item["created_by_user_id"], item["status"],
                    item["assigned_performer_id"], item["close_reason"], item["failure_reason"],
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
            SET status = %s, assigned_performer_id = %s, close_reason = %s,
                failure_reason = %s, auto_closed = %s, auto_close_reason = %s, updated_at = %s
            WHERE id = %s
            RETURNING *
            """,
            (
                shift["status"], shift["assigned_performer_id"], shift["close_reason"],
                shift["failure_reason"], shift["auto_closed"], shift["auto_close_reason"],
                shift["updated_at"], shift["id"],
            ),
        ).fetchone()
        return normalize_row(row)  # type: ignore[return-value]

    def insert_history(self, connection, item: dict[str, Any]) -> dict[str, Any]:
        row = connection.execute(
            """
            INSERT INTO shift_status_history
                (id, shift_id, from_status, to_status, transition_code, reason,
                 actor_user_id, occurred_at, correlation_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                item["id"], item["shift_id"], item["from_status"], item["to_status"],
                item["transition_code"], item["reason"], item["actor_user_id"],
                item["occurred_at"], item["correlation_id"], json_value(item["metadata"]),
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
                INSERT INTO offer_campaigns (id, shift_id, status, created_by_user_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (campaign["id"], campaign["shift_id"], campaign["status"], campaign["created_by_user_id"], campaign["created_at"]),
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
                    (id, shift_id, performer_id, offer_id, status, assigned_at, removed_at, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    assignment["id"], assignment["shift_id"], assignment["performer_id"], assignment["offer_id"],
                    assignment["status"], assignment["assigned_at"], assignment["removed_at"], assignment["reason"],
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
            shift["status"] = "A30_CHOICE"
            shift["updated_at"] = assignment["assigned_at"]
            shift = self.update_shift(connection, shift)
            history = self.insert_history(connection, history)
            return {
                "conflict": False,
                "offer": offer,
                "assignment": assignment,
                "shift": shift,
                "history": history,
                "lost_offers": [normalize_row(row) for row in lost_offers],
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
