import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import unittest
from uuid import uuid4


DATABASE_URL = os.getenv("OMS5_TEST_DATABASE_URL")


@unittest.skipUnless(DATABASE_URL, "OMS5_TEST_DATABASE_URL is required")
class OperationalStoragePostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from app.settings import settings
        from app.storage import storage

        settings.database_url = DATABASE_URL
        storage.migrate()
        cls.storage = storage

    def setUp(self):
        self.client_id = str(uuid4())
        self.performer_ids = [str(uuid4()), str(uuid4())]
        self.person_ids = [int(uuid4().int % 1000000000), int(uuid4().int % 1000000000)]
        self.shift_id = str(uuid4())
        self.offer_ids = [str(uuid4()), str(uuid4())]
        self.campaign_id = str(uuid4())
        self.token_hashes = [uuid4().hex.ljust(64, "0"), uuid4().hex.ljust(64, "0")]
        now = datetime.now(timezone.utc)
        self.storage.create_client({"id": self.client_id, "name": "storage-test", "external_id": None, "client_type_code": "object", "parent_client_id": None, "created_at": now})
        for performer_id, person_id in zip(self.performer_ids, self.person_ids):
            self.storage.sync_performer(
                {
                    "id": performer_id,
                    "person_id": person_id,
                    "display_name": f"Storage Test {person_id}",
                    "email": None,
                    "phone": None,
                    "status": "active",
                    "source_version": 1,
                    "synced_at": now,
                    "engagement_type": "self_employed",
                    "engagement_type_validation_status": "verified",
                    "funnel_stage": "PERFORMER_READY_FOOD",
                    "medical_book_status": "food",
                    "completed_shifts_count": 0,
                },
                created=True,
            )
        self.storage.create_shift(
            {
                "id": self.shift_id,
                "client_id": self.client_id,
                "starts_at": datetime(2027, 1, 20, 9, tzinfo=timezone.utc),
                "ends_at": datetime(2027, 1, 20, 18, tzinfo=timezone.utc),
                "location": "storage-test",
                "required_performers_count": 1,
                "created_by_user_id": None,
                "status": "A20_SOURCING",
                "assigned_performer_id": None,
                "close_reason": None,
                "failure_reason": None,
                "auto_closed": False,
                "auto_close_reason": None,
                "created_at": now,
                "updated_at": now,
            }
        )
        self.storage.create_campaign(
            {
                "id": self.campaign_id,
                "shift_id": self.shift_id,
                "status": "sent",
                "created_by_user_id": "test",
                "created_at": now,
            },
            [
                {
                    "id": offer_id,
                    "campaign_id": self.campaign_id,
                    "shift_id": self.shift_id,
                    "performer_id": performer_id,
                    "status": "sent",
                    "token_hash": token_hash,
                    "expires_at": datetime(2027, 1, 20, 9, tzinfo=timezone.utc),
                    "sent_at": now,
                    "responded_at": None,
                }
                for offer_id, performer_id, token_hash in zip(self.offer_ids, self.performer_ids, self.token_hashes)
            ],
        )

    def tearDown(self):
        with self.storage.connection() as connection:
            connection.execute("DELETE FROM operations_outbox WHERE person_id = ANY(%s)", (self.person_ids,))
            connection.execute("DELETE FROM shifts WHERE client_id = %s", (self.client_id,))
            connection.execute("DELETE FROM clients WHERE id = %s", (self.client_id,))
            connection.execute("DELETE FROM performers WHERE id = ANY(%s)", (self.performer_ids,))

    def test_migration_is_idempotent(self):
        self.storage.migrate()
        with self.storage.connection() as connection:
            tables = connection.execute(
                "SELECT count(*) AS count FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'shifts'"
            ).fetchone()
        self.assertEqual(tables["count"], 1)

    def test_concurrent_accept_locks_shift_and_creates_one_assignment(self):
        def accept(index: int):
            now = datetime.now(timezone.utc)
            return self.storage.accept_offer(
                self.token_hashes[index],
                {
                    "id": str(uuid4()),
                    "shift_id": self.shift_id,
                    "performer_id": self.performer_ids[index],
                    "offer_id": self.offer_ids[index],
                    "status": "assigned",
                    "assigned_at": now,
                    "removed_at": None,
                    "reason": None,
                },
                {
                    "id": str(uuid4()),
                    "shift_id": self.shift_id,
                    "from_status": "A20_SOURCING",
                    "to_status": "A30_CHOICE",
                    "transition_code": "A20_SOURCING->A30_CHOICE",
                    "reason": "first accepted wins",
                    "actor_user_id": None,
                    "occurred_at": now,
                    "correlation_id": str(uuid4()),
                    "metadata": {},
                },
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(accept, (0, 1)))

        self.assertEqual(sum(not result["conflict"] for result in results), 1)
        self.assertEqual(sum(result["conflict"] for result in results), 1)
        with self.storage.connection() as connection:
            assignment_count = connection.execute(
                "SELECT count(*) AS count FROM shift_assignments WHERE shift_id = %s", (self.shift_id,)
            ).fetchone()["count"]
            history_count = connection.execute(
                "SELECT count(*) AS count FROM shift_status_history WHERE shift_id = %s", (self.shift_id,)
            ).fetchone()["count"]
        self.assertEqual(assignment_count, 1)
        self.assertEqual(history_count, 1)

    def accept_first_offer(self) -> dict:
        now = datetime.now(timezone.utc)
        return self.storage.accept_offer(
            self.token_hashes[0],
            {
                "id": str(uuid4()),
                "shift_id": self.shift_id,
                "performer_id": self.performer_ids[0],
                "offer_id": self.offer_ids[0],
                "status": "assigned",
                "assigned_at": now,
                "removed_at": None,
                "reason": None,
            },
            {
                "id": str(uuid4()),
                "shift_id": self.shift_id,
                "from_status": "A20_SOURCING",
                "to_status": "A30_CHOICE",
                "transition_code": "A20_SOURCING->A30_CHOICE",
                "reason": "first accepted wins",
                "actor_user_id": None,
                "occurred_at": now,
                "correlation_id": str(uuid4()),
                "metadata": {},
            },
        )

    def test_accept_writes_exactly_one_assignment_fact_with_snapshot(self):
        from app.funnel_outbox import FACT_ASSIGNED, enqueue_fact

        result = self.accept_first_offer()
        self.assertFalse(result["conflict"])
        self.assertEqual(result["assignment"]["engagement_type"], "self_employed")

        with self.storage.connection() as connection:
            facts = connection.execute(
                "SELECT event_type, person_id, assignment_id, payload, published_at FROM operations_outbox WHERE assignment_id = %s",
                (result["assignment"]["id"],),
            ).fetchall()
            snapshot = connection.execute(
                "SELECT engagement_type, completed_at FROM shift_assignments WHERE id = %s",
                (result["assignment"]["id"],),
            ).fetchone()
            shift_row = connection.execute(
                "SELECT assigned_performer_person_id FROM shifts WHERE id = %s", (self.shift_id,)
            ).fetchone()
            duplicate = enqueue_fact(
                connection,
                event_type=FACT_ASSIGNED,
                person_id=self.person_ids[0],
                assignment_id=result["assignment"]["id"],
                occurred_at=datetime.now(timezone.utc),
                payload={},
            )
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["event_type"], "operations.shift.assigned")
        self.assertEqual(facts[0]["person_id"], self.person_ids[0])
        self.assertIsNone(facts[0]["published_at"])
        self.assertEqual(facts[0]["payload"]["aggregate_type"], "performer")
        self.assertEqual(facts[0]["payload"]["aggregate_id"], str(self.person_ids[0]))
        self.assertEqual(facts[0]["payload"]["payload"]["engagement_type"], "self_employed")
        self.assertNotIn("aggregate_version", facts[0]["payload"])
        self.assertEqual(snapshot["engagement_type"], "self_employed")
        self.assertIsNone(snapshot["completed_at"])
        self.assertEqual(shift_row["assigned_performer_person_id"], self.person_ids[0])
        self.assertIsNone(duplicate)

    def test_repeated_accept_does_not_publish_second_fact(self):
        self.accept_first_offer()
        second = self.accept_first_offer()
        self.assertTrue(second["conflict"])
        with self.storage.connection() as connection:
            count = connection.execute(
                "SELECT count(*) AS count FROM operations_outbox WHERE person_id = %s", (self.person_ids[0],)
            ).fetchone()["count"]
        self.assertEqual(count, 1)

    def test_completion_fact_is_created_once_and_snapshots_engagement_type(self):
        from app.funnel_outbox import enqueue_fact

        accepted = self.accept_first_offer()
        self.storage.create_timesheet(
            {
                "id": str(uuid4()),
                "performer_id": self.performer_ids[0],
                "shift_id": self.shift_id,
                "work_date": date(2027, 1, 20),
                "hours": 8.0,
                "status": "submitted",
                "created_at": datetime.now(timezone.utc),
            }
        )
        occurred = datetime.now(timezone.utc)
        with self.storage.connection() as connection:
            shift = self.storage.get_shift(self.shift_id, for_update=True, connection=connection)
            fact = self.storage.prepare_shift_completion(connection, shift, occurred)
            self.assertIsNotNone(fact)
            self.assertIsNotNone(enqueue_fact(connection, **fact))
            self.assertIsNone(self.storage.prepare_shift_completion(connection, shift, occurred))
            completed_at = connection.execute(
                "SELECT completed_at FROM shift_assignments WHERE id = %s", (accepted["assignment"]["id"],)
            ).fetchone()["completed_at"]
            facts = connection.execute(
                "SELECT event_type, payload FROM operations_outbox WHERE assignment_id = %s ORDER BY created_at",
                (accepted["assignment"]["id"],),
            ).fetchall()
        self.assertIsNotNone(completed_at)
        self.assertEqual([row["event_type"] for row in facts], ["operations.shift.assigned", "operations.shift.completed"])
        completed_payload = facts[1]["payload"]["payload"]
        self.assertEqual(completed_payload["engagement_type"], "self_employed")
        self.assertEqual(completed_payload["assignment_id"], accepted["assignment"]["id"])
        self.assertEqual(completed_payload["hours"], 8.0)
        self.assertEqual(len(completed_payload["timesheets"]), 1)
        self.assertEqual(completed_payload["timesheets"][0]["work_date"], "2027-01-20")

    def test_kafka_outage_keeps_fact_until_publisher_runs(self):
        import json

        from app.funnel_outbox import dispatch_outbox_once

        accepted = self.accept_first_offer()

        def unavailable():
            raise RuntimeError("kafka unavailable")

        published = dispatch_outbox_once(producer_factory=unavailable)
        self.assertEqual(published, 0)
        with self.storage.connection() as connection:
            row = connection.execute(
                "SELECT published_at, attempts, last_error FROM operations_outbox WHERE assignment_id = %s",
                (accepted["assignment"]["id"],),
            ).fetchone()
        self.assertIsNone(row["published_at"])
        self.assertEqual(row["attempts"], 1)
        self.assertIn("kafka unavailable", row["last_error"])

        class RecordingProducer:
            def __init__(self):
                self.messages: list[tuple[str, bytes | None, bytes]] = []

            def produce(self, topic, key=None, value=None, on_delivery=None):
                self.messages.append((topic, key, value))
                if on_delivery is not None:
                    on_delivery(None, None)

            def flush(self, timeout=None):
                return 0

        producer = RecordingProducer()
        self.assertEqual(dispatch_outbox_once(producer_factory=lambda: producer), 1)
        with self.storage.connection() as connection:
            row = connection.execute(
                "SELECT published_at FROM operations_outbox WHERE assignment_id = %s",
                (accepted["assignment"]["id"],),
            ).fetchone()
        self.assertIsNotNone(row["published_at"])
        self.assertEqual(len(producer.messages), 1)
        topic, key, value = producer.messages[0]
        self.assertEqual(topic, "operations.performer.v1")
        self.assertEqual(key, str(self.person_ids[0]).encode("utf-8"))
        envelope = json.loads(value.decode("utf-8"))
        self.assertEqual(envelope["event_type"], "operations.shift.assigned")
        self.assertEqual(envelope["aggregate_id"], str(self.person_ids[0]))

        self.assertEqual(dispatch_outbox_once(producer_factory=lambda: producer), 0)
        self.assertEqual(len(producer.messages), 1)

    def test_projection_applies_funnel_fields_and_keeps_completed_shifts_max(self):
        person_id = int(uuid4().int % 1000000000)
        self.person_ids.append(person_id)

        def envelope(version: int, payload: dict) -> dict:
            return {
                "event_id": str(uuid4()),
                "event_type": "people.performer_profile.changed",
                "schema_version": 1,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "aggregate_type": "person",
                "aggregate_id": str(person_id),
                "aggregate_version": version,
                "payload": {"person_id": str(person_id), **payload},
            }

        base = {"profile_status": "active", "display_name": "Funnel Person"}
        try:
            self.assertEqual(
                self.storage.apply_person_event(envelope(1, {
                    **base,
                    "engagement_type": "gph",
                    "engagement_type_validation_status": "pending",
                    "funnel_stage": "PERFORMER_PROFILE",
                    "medical_book_status": "none",
                    "completed_shifts_count": 0,
                })),
                "applied",
            )
            performer = self.storage.get_performer_by_person_id(person_id)
            self.assertEqual(performer["engagement_type"], "gph")
            self.assertEqual(performer["engagement_type_validation_status"], "pending")
            self.assertEqual(performer["funnel_stage"], "PERFORMER_PROFILE")

            self.assertEqual(
                self.storage.apply_person_event(envelope(2, {
                    **base,
                    "engagement_type_validation_status": "verified",
                    "funnel_stage": "PERFORMER_FIRST_SHIFT_COMPLETED",
                    "completed_shifts_count": 3,
                })),
                "applied",
            )
            performer = self.storage.get_performer_by_person_id(person_id)
            self.assertEqual(performer["completed_shifts_count"], 3)
            self.assertEqual(performer["funnel_stage"], "PERFORMER_FIRST_SHIFT_COMPLETED")

            self.assertEqual(
                self.storage.apply_person_event(envelope(3, {**base, "completed_shifts_count": 1})),
                "applied",
            )
            self.assertEqual(self.storage.get_performer_by_person_id(person_id)["completed_shifts_count"], 3)

            self.assertEqual(
                self.storage.apply_person_event(envelope(4, {
                    **base,
                    "engagement_type": "self_employed",
                    "funnel_stage": "PERFORMER_UNKNOWN_STAGE",
                })),
                "applied",
            )
            performer = self.storage.get_performer_by_person_id(person_id)
            self.assertEqual(performer["engagement_type"], "self_employed")
            self.assertEqual(performer["funnel_stage"], "PERFORMER_FIRST_SHIFT_COMPLETED")
        finally:
            with self.storage.connection() as connection:
                connection.execute("DELETE FROM person_event_inbox WHERE aggregate_id = %s", (person_id,))
                connection.execute("DELETE FROM person_consumer_state WHERE aggregate_id = %s", (person_id,))
                connection.execute("DELETE FROM performers WHERE person_id = %s", (person_id,))


    def test_person_event_ordering_and_merge(self):
        from uuid import uuid4 as new_uuid

        duplicate_person_id = int(new_uuid().int % 1000000000)
        canonical_person_id = duplicate_person_id + 1

        def envelope(version: int, aggregate_id: int, payload: dict, event_type: str = "people.performer_profile.changed") -> dict:
            return {
                "event_id": str(new_uuid()),
                "event_type": event_type,
                "schema_version": 1,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "aggregate_type": "person",
                "aggregate_id": str(aggregate_id),
                "aggregate_version": version,
                "payload": payload,
            }

        try:
            first = envelope(1, duplicate_person_id, {"person_id": str(duplicate_person_id), "profile_status": "active", "display_name": "Duplicate Person"})
            self.assertEqual(self.storage.apply_person_event(first), "applied")
            self.assertEqual(self.storage.apply_person_event(first), "duplicate")

            gap = envelope(3, duplicate_person_id, {"person_id": str(duplicate_person_id), "profile_status": "active", "display_name": "Skipped"})
            self.assertEqual(self.storage.apply_person_event(gap), "gap")
            duplicate = self.storage.get_performer_by_person_id(duplicate_person_id)
            self.assertEqual(duplicate["display_name"], "Duplicate Person")

            second = envelope(2, duplicate_person_id, {"person_id": str(duplicate_person_id), "profile_status": "active", "display_name": "Renamed Person"})
            self.assertEqual(self.storage.apply_person_event(second), "applied")
            self.assertEqual(self.storage.get_performer_by_person_id(duplicate_person_id)["display_name"], "Renamed Person")
            self.assertEqual(self.storage.apply_person_event(gap), "applied")

            canonical_event = envelope(1, canonical_person_id, {"person_id": str(canonical_person_id), "profile_status": "active", "display_name": "Canonical Person"})
            self.assertEqual(self.storage.apply_person_event(canonical_event), "applied")

            merge_event = envelope(
                4,
                duplicate_person_id,
                {"duplicate_person_id": str(duplicate_person_id), "canonical_person_id": str(canonical_person_id), "merged_at": datetime.now(timezone.utc).isoformat(), "reason_code": "verified_identifier_match"},
                event_type="people.merged",
            )
            self.assertEqual(self.storage.apply_person_event(merge_event), "applied")
            self.assertEqual(self.storage.apply_person_event(merge_event), "duplicate")

            duplicate = self.storage.get_performer(duplicate["id"])
            self.assertEqual(duplicate["status"], "merged")
            canonical = self.storage.get_performer_by_person_id(canonical_person_id)
            self.assertEqual(duplicate["merged_into_performer_id"], canonical["id"])
        finally:
            with self.storage.connection() as connection:
                connection.execute("DELETE FROM person_event_inbox WHERE aggregate_id = ANY(%s)", ([duplicate_person_id, canonical_person_id],))
                connection.execute("DELETE FROM person_consumer_state WHERE aggregate_id = ANY(%s)", ([duplicate_person_id, canonical_person_id],))
                connection.execute("DELETE FROM performers WHERE person_id = ANY(%s)", ([duplicate_person_id, canonical_person_id],))


if __name__ == "__main__":
    unittest.main()
