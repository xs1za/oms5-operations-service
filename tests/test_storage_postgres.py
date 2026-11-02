import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
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
