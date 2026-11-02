import os
from datetime import datetime, timedelta, timezone
import unittest
from uuid import uuid4

from fastapi import HTTPException


DATABASE_URL = os.getenv("OMS5_TEST_DATABASE_URL")


@unittest.skipUnless(DATABASE_URL, "OMS5_TEST_DATABASE_URL is required")
class OperationalApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from app import main as api
        from app.settings import settings
        from app.storage import storage

        settings.database_url = DATABASE_URL
        settings.oms2_service_secret = "service-secret"
        storage.migrate()
        cls.api = api
        cls.storage = storage
        api.emit = lambda *args, **kwargs: None

    def setUp(self):
        self.head_id = str(uuid4())
        self.division_id = str(uuid4())
        self.object_id = str(uuid4())
        self.other_head_id = str(uuid4())
        self.other_object_id = str(uuid4())
        now = datetime.now(timezone.utc)
        self.storage.create_client({"id": self.head_id, "name": "Head", "external_id": None, "client_type_code": "head", "parent_client_id": None, "created_at": now})
        self.storage.create_client({"id": self.division_id, "name": "Division", "external_id": None, "client_type_code": "division", "parent_client_id": self.head_id, "created_at": now})
        self.storage.create_client({"id": self.object_id, "name": "Object", "external_id": None, "client_type_code": "object", "parent_client_id": self.division_id, "created_at": now})
        self.storage.create_client({"id": self.other_head_id, "name": "Other Head", "external_id": None, "client_type_code": "head", "parent_client_id": None, "created_at": now})
        self.storage.create_client({"id": self.other_object_id, "name": "Other Object", "external_id": None, "client_type_code": "object", "parent_client_id": None, "created_at": now})

        self.person_id = int(uuid4().int % 1000000000)
        self.performer_id = str(uuid4())
        self.storage.sync_performer(
            {
                "id": self.performer_id,
                "person_id": self.person_id,
                "display_name": "Self Operator",
                "email": None,
                "phone": None,
                "status": "active",
                "source_version": 1,
                "synced_at": now,
            },
            created=True,
        )

    def tearDown(self):
        with self.storage.connection() as connection:
            connection.execute("DELETE FROM shifts WHERE client_id = ANY(%s)", ([self.object_id, self.other_object_id],))
            connection.execute("DELETE FROM clients WHERE id = ANY(%s)", ([self.head_id, self.division_id, self.object_id, self.other_head_id, self.other_object_id],))
            connection.execute("DELETE FROM performers WHERE id = %s", (self.performer_id,))

    def make_shift(self) -> dict:
        starts_at = datetime.now(timezone.utc) + timedelta(days=30)
        now = datetime.now(timezone.utc)
        return self.storage.create_shift(
            {
                "id": str(uuid4()),
                "client_id": self.object_id,
                "starts_at": starts_at,
                "ends_at": starts_at + timedelta(hours=9),
                "location": "test",
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

    def test_self_assignment_is_rejected_without_side_effects(self):
        shift = self.make_shift()
        principal = {
            "user_id": str(uuid4()),
            "person_id": str(self.person_id),
            "display_name": "Self Operator",
            "roles": ["manager"],
            "type": "user",
        }
        with self.assertRaises(HTTPException) as context:
            self.api.create_offer_campaign(
                shift["id"],
                self.api.OfferCampaignCreate(performer_ids=[self.performer_id]),
                principal=principal,
            )
        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail["code"], "self_assignment_not_allowed")
        with self.storage.connection() as connection:
            campaigns = connection.execute(
                "SELECT count(*) AS count FROM offer_campaigns WHERE shift_id = %s", (shift["id"],)
            ).fetchone()["count"]
        self.assertEqual(campaigns, 0)

    def test_external_user_cannot_create_shift_outside_organization(self):
        principal = {
            "user_id": str(uuid4()),
            "person_id": str(self.person_id),
            "display_name": "External User",
            "roles": ["external_user"],
            "organization_id": self.head_id,
            "type": "user",
        }
        starts_at = datetime.now(timezone.utc) + timedelta(days=30)
        with self.assertRaises(HTTPException) as context:
            self.api.create_shift(
                self.api.ShiftCreate(
                    client_id=self.other_object_id,
                    starts_at=starts_at,
                    ends_at=starts_at + timedelta(hours=9),
                ),
                principal=principal,
            )
        self.assertEqual(context.exception.status_code, 403)
        self.assertEqual(context.exception.detail["code"], "organization_scope_violation")

    def test_external_user_creates_shift_inside_organization(self):
        principal = {
            "user_id": str(uuid4()),
            "person_id": str(self.person_id),
            "display_name": "External User",
            "roles": ["external_user"],
            "organization_id": self.head_id,
            "type": "user",
        }
        starts_at = datetime.now(timezone.utc) + timedelta(days=30)
        created = self.api.create_shift(
            self.api.ShiftCreate(
                client_id=self.object_id,
                starts_at=starts_at,
                ends_at=starts_at + timedelta(hours=9),
            ),
            principal=principal,
        )
        self.assertEqual(created["created_by_person_id"], str(self.person_id))
        self.assertEqual(created["created_by_display_name"], "External User")

    def test_object_must_be_division_child_or_autonomous(self):
        now = datetime.now(timezone.utc)
        with self.assertRaises(HTTPException) as context:
            self.api.create_client(
                self.api.ClientCreate(
                    name="Invalid",
                    client_type_code="object",
                    parent_client_id=self.head_id,
                )
            )
        self.assertEqual(context.exception.status_code, 422)
        self.assertEqual(context.exception.detail["code"], "invalid_parent_type")

    def test_campaign_response_hides_storage_fields(self):
        shift = self.make_shift()
        actor_person_id = self.person_id + 1
        principal = {
            "user_id": str(uuid4()),
            "person_id": str(actor_person_id),
            "display_name": "Другой Оператор",
            "roles": ["manager"],
            "type": "user",
        }
        result = self.api.create_offer_campaign(
            shift["id"],
            self.api.OfferCampaignCreate(performer_ids=[self.performer_id]),
            principal=principal,
        )

        offer = result["offers"][0]
        self.assertNotIn("token_hash", offer)
        self.assertNotIn("responded_at", offer)
        self.assertIn("token", offer)
        self.assertIn("public_url", offer)
        self.assertTrue(offer["expires_at"].endswith("Z"))

        campaign = result["campaign"]
        self.assertNotIn("token_hash", campaign)
        self.assertEqual(campaign["created_by_person_id"], str(actor_person_id))
        self.assertEqual(campaign["created_by_display_name"], "Другой Оператор")
        self.assertTrue(campaign["created_at"].endswith("Z"))

    def test_client_hierarchy_is_limited_to_three_levels(self):
        deep_id = str(uuid4())
        now = datetime.now(timezone.utc)
        self.storage.create_client({"id": deep_id, "name": "Deep", "external_id": None, "client_type_code": "object", "parent_client_id": self.division_id, "created_at": now})
        try:
            with self.assertRaises(HTTPException) as context:
                self.api.create_client(
                    self.api.ClientCreate(
                        name="Too Deep",
                        client_type_code="object",
                        parent_client_id=deep_id,
                    )
                )
            self.assertEqual(context.exception.status_code, 422)
        finally:
            with self.storage.connection() as connection:
                connection.execute("DELETE FROM clients WHERE id = %s", (deep_id,))

    def test_performer_sync_requires_service_token(self):
        with self.assertRaises(HTTPException) as context:
            self.api.require_service_token("wrong")
        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(context.exception.detail["code"], "service_token_required")


if __name__ == "__main__":
    unittest.main()
