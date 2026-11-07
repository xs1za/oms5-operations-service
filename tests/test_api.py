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
        api.publish_event = lambda *args, **kwargs: None

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
        self.person_ids = [self.person_id]
        self.performer_ids = [self.performer_id]
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
                "engagement_type": "self_employed",
                "engagement_type_validation_status": "verified",
                "funnel_stage": "PERFORMER_READY_FOOD",
                "medical_book_status": "food",
                "completed_shifts_count": 0,
            },
            created=True,
        )

    def create_performer(self, display_name: str, **values) -> tuple[str, int]:
        now = datetime.now(timezone.utc)
        person_id = int(uuid4().int % 1000000000)
        performer_id = str(uuid4())
        item = {
            "id": performer_id,
            "person_id": person_id,
            "display_name": display_name,
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
            **values,
        }
        self.storage.sync_performer(item, created=True)
        self.person_ids.append(person_id)
        self.performer_ids.append(performer_id)
        return performer_id, person_id

    def update_performer(self, performer_id: str, **values) -> None:
        assignments = ", ".join(f"{column} = %s" for column in values)
        with self.storage.connection() as connection:
            connection.execute(
                f"UPDATE performers SET {assignments} WHERE id = %s",
                [*values.values(), performer_id],
            )

    def tearDown(self):
        with self.storage.connection() as connection:
            connection.execute("DELETE FROM operations_outbox WHERE person_id = ANY(%s)", (self.person_ids,))
            connection.execute("DELETE FROM shifts WHERE client_id = ANY(%s)", ([self.object_id, self.other_object_id],))
            connection.execute("DELETE FROM clients WHERE id = ANY(%s)", ([self.head_id, self.division_id, self.object_id, self.other_head_id, self.other_object_id],))
            connection.execute("DELETE FROM performers WHERE id = ANY(%s)", (self.performer_ids,))

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

    def test_performer_read_endpoints_require_service_token(self):
        from fastapi.routing import APIRoute

        routes = {
            route.path: route
            for route in self.api.app.routes
            if isinstance(route, APIRoute)
        }
        for path in ("/performers/by-person/{person_id}", "/performers/{performer_id}"):
            dependency_calls = [dependency.call for dependency in routes[path].dependant.dependencies]
            self.assertIn(self.api.require_service_token, dependency_calls)

        for token in (None, "wrong"):
            with self.assertRaises(HTTPException) as context:
                self.api.require_service_token(token)
            self.assertEqual(context.exception.status_code, 401)
            self.assertEqual(context.exception.detail["code"], "service_token_required")
            self.assertEqual(context.exception.detail["message"], "A valid service token is required")
        self.assertIsNone(self.api.require_service_token("service-secret"))

    def test_performer_projection_is_returned_by_read_endpoints(self):
        by_person = self.api.get_performer_by_person(str(self.person_id))
        by_id = self.api.get_performer(self.performer_id)
        for payload in (by_person, by_id):
            self.assertEqual(payload["engagement_type"], "self_employed")
            self.assertEqual(payload["engagement_type_validation_status"], "verified")
            self.assertEqual(payload["funnel_stage"], "PERFORMER_READY_FOOD")
            self.assertEqual(payload["medical_book_status"], "food")
            self.assertEqual(payload["completed_shifts_count"], 0)
            self.assertEqual(payload["person_id"], str(self.person_id))
            self.assertTrue(payload["synced_at"].endswith("Z"))

        with self.assertRaises(HTTPException) as missing_by_person:
            self.api.get_performer_by_person("999999999999")
        self.assertEqual(missing_by_person.exception.status_code, 404)
        self.assertEqual(missing_by_person.exception.detail["code"], "performer_not_found")
        self.assertEqual(missing_by_person.exception.detail["message"], "Performer not found")

        with self.assertRaises(HTTPException) as missing_by_id:
            self.api.get_performer(str(uuid4()))
        self.assertEqual(missing_by_id.exception.status_code, 404)
        self.assertEqual(missing_by_id.exception.detail["code"], "performer_not_found")
        self.assertEqual(missing_by_id.exception.detail["message"], "Performer not found")

        with self.assertRaises(HTTPException) as invalid_uuid:
            self.api.get_performer("not-a-uuid")
        self.assertEqual(invalid_uuid.exception.status_code, 422)
        self.assertEqual(invalid_uuid.exception.detail["code"], "invalid_uuid_format")
        self.assertEqual(invalid_uuid.exception.detail["message"], "performer_id must be a UUID string")

        with self.assertRaises(HTTPException) as invalid_person:
            self.api.get_performer_by_person("12a")
        self.assertEqual(invalid_person.exception.status_code, 422)
        self.assertEqual(invalid_person.exception.detail["code"], "validation_error")
        self.assertEqual(invalid_person.exception.detail["message"], "person_id must be a decimal BIGINT string")

    def test_merged_performer_is_hidden_by_person_and_returned_by_id(self):
        canonical_id, _ = self.create_performer("Canonical")
        self.update_performer(
            self.performer_id,
            status="merged",
            merged_into_performer_id=canonical_id,
        )

        with self.assertRaises(HTTPException) as missing:
            self.api.get_performer_by_person(str(self.person_id))
        self.assertEqual(missing.exception.status_code, 404)
        self.assertEqual(missing.exception.detail["code"], "performer_not_found")

        merged = self.api.get_performer(self.performer_id)
        self.assertEqual(merged["merged_into_performer_id"], canonical_id)

    def operator_principal(self) -> dict:
        return {
            "user_id": str(uuid4()),
            "person_id": str(self.person_id + 1),
            "display_name": "Оператор",
            "roles": ["manager"],
            "type": "user",
        }

    def campaign(self, shift_id: str, performer_ids: list[str]) -> dict:
        return self.api.create_offer_campaign(
            shift_id,
            self.api.OfferCampaignCreate(performer_ids=performer_ids),
            principal=self.operator_principal(),
        )

    def assert_campaign_not_created(self, shift_id: str) -> None:
        with self.storage.connection() as connection:
            campaigns = connection.execute(
                "SELECT count(*) AS count FROM offer_campaigns WHERE shift_id = %s", (shift_id,)
            ).fetchone()["count"]
            offers = connection.execute(
                "SELECT count(*) AS count FROM shift_offers WHERE shift_id = %s", (shift_id,)
            ).fetchone()["count"]
        self.assertEqual(campaigns, 0)
        self.assertEqual(offers, 0)

    def test_offer_campaign_rejects_pending_engagement_type(self):
        self.update_performer(self.performer_id, engagement_type_validation_status="pending")
        shift = self.make_shift()
        with self.assertRaises(HTTPException) as context:
            self.campaign(shift["id"], [self.performer_id])
        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail["code"], "performer_not_ready")
        self.assertIn("engagement_type_not_verified", context.exception.detail["reasons"])
        self.assertEqual(context.exception.detail["performer_id"], self.performer_id)
        self.assertEqual(context.exception.detail["person_id"], str(self.person_id))
        self.assert_campaign_not_created(shift["id"])

    def test_offer_campaign_rejects_rejected_engagement_type(self):
        self.update_performer(self.performer_id, engagement_type_validation_status="rejected")
        shift = self.make_shift()
        with self.assertRaises(HTTPException) as context:
            self.campaign(shift["id"], [self.performer_id])
        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail["code"], "performer_not_ready")
        self.assertIn("engagement_type_not_verified", context.exception.detail["reasons"])
        self.assert_campaign_not_created(shift["id"])

    def test_offer_campaign_rejects_docs_pending_stage(self):
        self.update_performer(self.performer_id, funnel_stage="PERFORMER_DOCS_PENDING")
        shift = self.make_shift()
        with self.assertRaises(HTTPException) as context:
            self.campaign(shift["id"], [self.performer_id])
        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail["code"], "performer_not_ready")
        self.assertIn("funnel_stage_not_ready", context.exception.detail["reasons"])
        self.assert_campaign_not_created(shift["id"])

    def test_offer_campaign_rejects_deleted_profile_stage(self):
        self.update_performer(self.performer_id, funnel_stage="PERFORMER_PROFILE_DELETED")
        shift = self.make_shift()
        with self.assertRaises(HTTPException):
            self.campaign(shift["id"], [self.performer_id])
        self.assert_campaign_not_created(shift["id"])

    def test_offer_campaign_is_not_created_partially(self):
        not_ready_id, _ = self.create_performer("Not Ready", funnel_stage="PERFORMER_PROFILE")
        shift = self.make_shift()
        with self.assertRaises(HTTPException) as context:
            self.campaign(shift["id"], [self.performer_id, not_ready_id])
        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail["code"], "performer_not_ready")
        self.assert_campaign_not_created(shift["id"])

    def test_offer_campaign_for_ready_food_performer_is_created(self):
        shift = self.make_shift()
        result = self.campaign(shift["id"], [self.performer_id])
        self.assertEqual(len(result["offers"]), 1)
        self.assertEqual(result["offers"][0]["performer_id"], self.performer_id)

    def test_accept_stores_engagement_snapshot_and_single_outbox_fact(self):
        shift = self.make_shift()
        campaign = self.campaign(shift["id"], [self.performer_id])
        token = campaign["offers"][0]["token"]

        result = self.api.accept_offer(token)

        self.assertEqual(result["assignment"]["engagement_type"], "self_employed")
        self.assertEqual(result["offer"], {"id": campaign["offers"][0]["id"], "status": "won"})
        self.assertNotIn("token_hash", result["offer"])
        self.assertNotIn("responded_at", result["offer"])
        self.assertNotIn("token_hash", result["assignment"])
        self.assertNotIn("responded_at", result["assignment"])
        self.assertEqual(result["shift"]["status"], "A30_CHOICE")

        with self.storage.connection() as connection:
            assignment = connection.execute(
                "SELECT engagement_type, completed_at FROM shift_assignments WHERE id = %s",
                (result["assignment"]["id"],),
            ).fetchone()
            facts = connection.execute(
                """
                SELECT event_type, person_id, assignment_id, payload, published_at
                FROM operations_outbox WHERE assignment_id = %s
                """,
                (result["assignment"]["id"],),
            ).fetchall()
        self.assertEqual(assignment["engagement_type"], "self_employed")
        self.assertIsNone(assignment["completed_at"])
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["event_type"], "operations.shift.assigned")
        self.assertEqual(facts[0]["person_id"], self.person_id)
        self.assertIsNone(facts[0]["published_at"])
        envelope = facts[0]["payload"]
        self.assertEqual(envelope["aggregate_id"], str(self.person_id))
        self.assertEqual(envelope["payload"]["assignment_id"], result["assignment"]["id"])
        self.assertEqual(envelope["payload"]["engagement_type"], "self_employed")

        self.update_performer(self.performer_id, engagement_type="gph")
        with self.storage.connection() as connection:
            snapshot = connection.execute(
                "SELECT engagement_type FROM shift_assignments WHERE id = %s",
                (result["assignment"]["id"],),
            ).fetchone()["engagement_type"]
        self.assertEqual(snapshot, "self_employed")

    def test_accept_without_engagement_type_is_rejected(self):
        shift = self.make_shift()
        campaign = self.campaign(shift["id"], [self.performer_id])
        token = campaign["offers"][0]["token"]
        self.update_performer(self.performer_id, engagement_type=None)
        with self.assertRaises(HTTPException) as context:
            self.api.accept_offer(token)
        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail["code"], "performer_not_ready")
        with self.storage.connection() as connection:
            assignments = connection.execute(
                "SELECT count(*) AS count FROM shift_assignments WHERE shift_id = %s", (shift["id"],)
            ).fetchone()["count"]
            facts = connection.execute(
                "SELECT count(*) AS count FROM operations_outbox WHERE person_id = %s", (self.person_id,)
            ).fetchone()["count"]
        self.assertEqual(assignments, 0)
        self.assertEqual(facts, 0)


if __name__ == "__main__":
    unittest.main()
