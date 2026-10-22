from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from hashlib import sha256
import logging
from secrets import token_urlsafe
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_serializer, field_validator

from app.healthcheck.router import router as healthcheck_router
from app.kafka import publish_event
from app.settings import settings
from app.storage import storage


app = FastAPI(title="OMS5 Operations Service", version="0.2.0", root_path=settings.root_path)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8088", "http://127.0.0.1:8088"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(healthcheck_router)
logger = logging.getLogger(settings.service_name)


class ShiftStatus(StrEnum):
    DRAFT = "A00_DRAFT"
    CONFIRM = "A10_CONFIRM"
    SOURCING = "A20_SOURCING"
    CHOICE = "A30_CHOICE"
    EXECUTION = "A40_EXECUTION"
    VERIFY = "A50_VERIFY"
    SETTLE = "A60_SETTLE"
    DOCS_CONFIRM = "A70_CONFIRM"
    CLOSED = "A80_CLOSED"
    ARCHIVE = "A90_ARCHIVE"


class CloseReason(StrEnum):
    PAID = "paid"
    CANCELED = "canceled"
    FAILED = "failed"
    DELETED = "deleted"


class FailureReason(StrEnum):
    ABSENCE = "absence"
    NO_PERFORMER_FOUND = "no_performer_found"
    CLIENT_REJECTED = "client_rejected"
    TIMESHEET_INVALID = "timesheet_invalid"
    MANUAL_ADMIN_DECISION = "manual_admin_decision"


class OfferStatus(StrEnum):
    DRAFT = "draft"
    SENT = "sent"
    VIEWED = "viewed"
    ACCEPTED = "accepted"
    DECLINED = "declined"
    WON = "won"
    LOST = "lost"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class CampaignStatus(StrEnum):
    DRAFT = "draft"
    SENT = "sent"
    CLOSED = "closed"
    CANCELLED = "cancelled"


ALLOWED_TRANSITIONS = {
    ShiftStatus.DRAFT: {ShiftStatus.CONFIRM},
    ShiftStatus.CONFIRM: {ShiftStatus.SOURCING},
    ShiftStatus.SOURCING: {ShiftStatus.CHOICE, ShiftStatus.CLOSED},
    ShiftStatus.CHOICE: {ShiftStatus.SOURCING, ShiftStatus.EXECUTION, ShiftStatus.CLOSED},
    ShiftStatus.EXECUTION: {ShiftStatus.VERIFY},
    ShiftStatus.VERIFY: {ShiftStatus.SETTLE, ShiftStatus.CLOSED},
    ShiftStatus.SETTLE: {ShiftStatus.DOCS_CONFIRM},
    ShiftStatus.DOCS_CONFIRM: {ShiftStatus.CLOSED},
    ShiftStatus.CLOSED: {ShiftStatus.ARCHIVE},
    ShiftStatus.ARCHIVE: set(),
}


class ClientCreate(BaseModel):
    name: str
    external_id: str | None = None


class PerformerSync(BaseModel):
    employee_id: int
    display_name: str
    email: str | None = None
    phone: str | None = None
    status: str = "active"
    source_version: int | None = None


class ShiftCreate(BaseModel):
    client_id: str
    starts_at: datetime
    ends_at: datetime
    location: str | None = None
    required_performers_count: int = Field(default=1, ge=1, le=1)
    created_by_user_id: str | None = None

    @field_validator("ends_at")
    @classmethod
    def validate_interval(cls, value: datetime, info) -> datetime:
        starts_at = info.data.get("starts_at")
        if starts_at and value <= starts_at:
            raise ValueError("ends_at must be greater than starts_at")
        return value


class ShiftTransition(BaseModel):
    new_status: ShiftStatus
    reason: str | None = None
    actor_user_id: str | None = None
    close_reason: CloseReason | None = None
    failure_reason: FailureReason | None = None


class InternalShiftRead(BaseModel):
    id: str
    client_id: str
    starts_at: datetime
    ends_at: datetime
    status: str
    location: str | None = None
    assigned_performer_id: str | None = None
    close_reason: str | None = None
    failure_reason: str | None = None
    created_at: datetime
    updated_at: datetime

    @field_serializer("starts_at", "ends_at", "created_at", "updated_at")
    def serialize_datetime(self, value: datetime) -> str:
        return utc_iso(as_aware(value))


class OfferCampaignCreate(BaseModel):
    performer_ids: list[str] = Field(min_length=1)
    created_by_user_id: str | None = None


class TimesheetCreate(BaseModel):
    performer_id: str
    shift_id: str
    work_date: date
    hours: float = Field(gt=0, le=24)


@app.on_event("startup")
def initialize_database() -> None:
    storage.migrate()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def as_aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def event_payload(event_type: str, payload: dict, correlation_id: str | None = None, actor: dict | None = None) -> dict:
    return {
        "eventId": f"evt_{uuid4().hex}",
        "eventType": event_type,
        "occurredAt": utcnow(),
        "producer": settings.service_name,
        "correlationId": correlation_id or f"corr_{uuid4().hex}",
        "actor": actor or {},
        "payload": payload,
    }


def emit(event_type: str, payload: dict, correlation_id: str | None = None, actor: dict | None = None) -> None:
    publish_event(event_type, event_payload(event_type, payload, correlation_id, actor))


def emit_shift_status_changed(
    shift_id: str,
    previous_status: str,
    new_status: ShiftStatus | str,
    reason: str | None,
    correlation_id: str,
) -> None:
    event = {
        "event_id": str(uuid4()),
        "event_type": "operations.shift.status_changed",
        "schema_version": 1,
        "occurred_at": utc_iso(utcnow()),
        "correlation_id": correlation_id,
        "producer": settings.service_name,
        "shift_id": shift_id,
        "previous_status": previous_status,
        "new_status": str(new_status),
        "reason": reason,
    }
    publish_event("operations.shift.status_changed", event, key=shift_id)


def token_hash(token: str) -> str:
    return sha256(token.encode("utf-8")).hexdigest()


def public_offer_url(token: str) -> str:
    return f"/shift-offers/{token}"


def reporting_period_close_datetime(starts_at: datetime) -> datetime:
    starts_at = as_aware(starts_at)
    year = starts_at.year + (1 if starts_at.month == 12 else 0)
    month = 1 if starts_at.month == 12 else starts_at.month + 1
    return datetime.combine(date(year, month, 10), time.max, tzinfo=timezone.utc)


def ensure_open_reporting_period(starts_at: datetime) -> None:
    if utcnow() > reporting_period_close_datetime(starts_at):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "reporting_period_closed", "message": "Cannot create shift in closed reporting period"},
        )


def make_history(shift: dict, new_status: ShiftStatus, reason: str | None = None, actor_user_id: str | None = None) -> dict:
    previous_status = shift["status"]
    if new_status not in ALLOWED_TRANSITIONS[ShiftStatus(previous_status)]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "invalid_shift_transition", "from": previous_status, "to": new_status},
        )
    return {
        "id": str(uuid4()),
        "shift_id": shift["id"],
        "from_status": previous_status,
        "to_status": new_status,
        "transition_code": f"{previous_status}->{new_status}",
        "reason": reason,
        "actor_user_id": actor_user_id,
        "occurred_at": utcnow(),
        "correlation_id": str(uuid4()),
        "metadata": {},
    }


def make_accept_history(shift_id: str) -> dict:
    now = utcnow()
    return {
        "id": str(uuid4()),
        "shift_id": shift_id,
        "from_status": ShiftStatus.SOURCING,
        "to_status": ShiftStatus.CHOICE,
        "transition_code": f"{ShiftStatus.SOURCING}->{ShiftStatus.CHOICE}",
        "reason": "first accepted wins",
        "actor_user_id": None,
        "occurred_at": now,
        "correlation_id": str(uuid4()),
        "metadata": {},
    }


def get_offer_context_or_404(token: str) -> tuple[dict, dict, dict]:
    context = storage.get_offer_context(token_hash(token))
    if context is None:
        raise HTTPException(status_code=404, detail={"code": "offer_token_not_found"})
    return context


def offer_availability(offer: dict, shift: dict) -> dict:
    if utcnow() >= as_aware(offer["expires_at"]):
        return {"available": False, "reason": "token_expired"}
    if shift.get("assigned_performer_id"):
        return {"available": False, "reason": "shift_already_assigned"}
    if offer["status"] in {OfferStatus.LOST, OfferStatus.EXPIRED, OfferStatus.CANCELLED, OfferStatus.DECLINED}:
        return {"available": False, "reason": f"offer_{offer['status']}"}
    if shift["status"] != ShiftStatus.SOURCING:
        return {"available": False, "reason": "shift_not_sourcing"}
    return {"available": True, "reason": None}


@app.post("/clients")
def create_client(payload: ClientCreate) -> dict:
    item = {"id": str(uuid4()), **payload.model_dump(), "created_at": utcnow()}
    item = storage.create_client(item)
    emit("operations.client_created", item)
    return item


@app.post("/performers/sync")
def sync_performer(payload: PerformerSync) -> dict:
    existing = storage.get_performer_by_employee_id(payload.employee_id)
    item = {
        "id": existing["id"] if existing else str(uuid4()),
        **payload.model_dump(),
        "synced_at": utcnow(),
    }
    item = storage.sync_performer(item, created=existing is None)
    emit("operations.performer.created" if existing is None else "operations.performer.updated", item)
    return item


@app.post("/shifts")
def create_shift(payload: ShiftCreate) -> dict:
    if storage.get_client(payload.client_id) is None:
        raise HTTPException(status_code=404, detail="Client not found")
    ensure_open_reporting_period(payload.starts_at)
    now = utcnow()
    item = {
        "id": str(uuid4()),
        **payload.model_dump(),
        "status": ShiftStatus.DRAFT,
        "assigned_performer_id": None,
        "close_reason": None,
        "failure_reason": None,
        "auto_closed": False,
        "auto_close_reason": None,
        "created_at": now,
        "updated_at": now,
    }
    item = storage.create_shift(item)
    emit("operations.shift.created", item)
    return item


@app.post("/shifts/{shift_id}/transition")
def transition_shift(shift_id: str, payload: ShiftTransition) -> dict:
    if payload.new_status == ShiftStatus.CLOSED and not payload.close_reason:
        raise HTTPException(status_code=422, detail="close_reason is required when closing shift")
    with storage.connection() as connection:
        shift = storage.get_shift(shift_id, for_update=True, connection=connection)
        if shift is None:
            raise HTTPException(status_code=404, detail="Shift not found")
        history = make_history(shift, payload.new_status, payload.reason, payload.actor_user_id)
        shift["status"] = payload.new_status
        shift["updated_at"] = utcnow()
        if payload.new_status == ShiftStatus.CLOSED:
            shift["close_reason"] = payload.close_reason
            shift["failure_reason"] = payload.failure_reason
        shift = storage.update_shift(connection, shift)
        history = storage.insert_history(connection, history)
    emit_shift_status_changed(shift_id, history["from_status"], history["to_status"], history["reason"], history["correlation_id"])
    if payload.new_status == ShiftStatus.CLOSED:
        emit("operations.shift.closed", {"shiftId": shift_id, "closeReason": shift["close_reason"], "failureReason": shift["failure_reason"]})
    return {"shift": shift, "history": history}


@app.get("/shifts/{shift_id}/history")
def get_shift_history(shift_id: str) -> list[dict]:
    if storage.get_shift(shift_id) is None:
        raise HTTPException(status_code=404, detail="Shift not found")
    return storage.get_history(shift_id)


@app.post("/shifts/{shift_id}/offer-campaigns")
def create_offer_campaign(shift_id: str, payload: OfferCampaignCreate) -> dict:
    shift = storage.get_shift(shift_id)
    if shift is None:
        raise HTTPException(status_code=404, detail="Shift not found")
    if shift["status"] != ShiftStatus.SOURCING:
        raise HTTPException(status_code=409, detail={"code": "shift_not_sourcing"})
    missing = [performer_id for performer_id in payload.performer_ids if storage.get_performer(performer_id) is None]
    if missing:
        raise HTTPException(status_code=404, detail={"code": "performers_not_found", "performerIds": missing})

    campaign = {
        "id": str(uuid4()),
        "shift_id": shift_id,
        "status": CampaignStatus.SENT,
        "created_by_user_id": payload.created_by_user_id,
        "created_at": utcnow(),
    }
    offers = []
    response_tokens = []
    for performer_id in payload.performer_ids:
        raw_token = token_urlsafe(32)
        response_tokens.append(raw_token)
        offers.append({
            "id": str(uuid4()), "campaign_id": campaign["id"], "shift_id": shift_id,
            "performer_id": performer_id, "status": OfferStatus.SENT, "token_hash": token_hash(raw_token),
            "expires_at": shift["starts_at"], "sent_at": utcnow(), "responded_at": None,
        })
    stored_offers = storage.create_campaign(campaign, offers)
    created_offers = []
    for offer, raw_token in zip(stored_offers, response_tokens):
        created_offers.append(offer | {"public_url": public_offer_url(raw_token), "token": raw_token})
        emit("operations.shift.offer_sent", {"shiftId": shift_id, "offerId": offer["id"], "performerId": offer["performer_id"]})
    emit("operations.shift.offer_campaign_created", {"shiftId": shift_id, "campaignId": campaign["id"]})
    return {"campaign": campaign, "offers": created_offers}


@app.get("/public/shift-offers/{token}")
def get_public_offer(token: str) -> dict:
    offer, shift, performer = get_offer_context_or_404(token)
    return {"offer": offer, "shift": shift, "performer": performer, **offer_availability(offer, shift)}


@app.post("/public/shift-offers/{token}/viewed")
def mark_offer_viewed(token: str) -> dict:
    correlation_id = f"corr_{uuid4().hex}"
    view = {"id": str(uuid4()), "viewed_at": utcnow(), "ip_address": None, "user_agent": None, "correlation_id": correlation_id}
    try:
        offer, shift = storage.mark_offer_viewed(token_hash(token), view)
    except KeyError:
        raise HTTPException(status_code=404, detail={"code": "offer_token_not_found"}) from None
    availability = offer_availability(offer, shift)
    emit("operations.shift.offer_viewed", {"shiftId": offer["shift_id"], "offerId": offer["id"], **availability}, correlation_id)
    return {"status": "logged", "offerStatus": offer["status"], "shiftStatus": shift["status"], **availability}


@app.post("/public/shift-offers/{token}/accept")
def accept_offer(token: str) -> dict:
    offer, shift, _ = get_offer_context_or_404(token)
    assignment = {
        "id": str(uuid4()), "shift_id": shift["id"], "performer_id": offer["performer_id"],
        "offer_id": offer["id"], "status": "assigned", "assigned_at": utcnow(), "removed_at": None, "reason": None,
    }
    history = make_accept_history(shift["id"])
    result = storage.accept_offer(token_hash(token), assignment, history)
    if result["conflict"]:
        reason = result.get("reason")
        if result["shift"].get("assigned_performer_id"):
            raise HTTPException(status_code=409, detail={"code": "offer_lost", "message": "Shift already assigned"})
        raise HTTPException(status_code=409, detail={"code": reason or "offer_unavailable"})

    accepted_offer = result["offer"]
    assigned_shift = result["shift"]
    for other_offer in result.get("lost_offers", []):
        emit("operations.shift.offer_lost", {"shiftId": assigned_shift["id"], "offerId": other_offer["id"], "performerId": other_offer["performer_id"]})
    emit_shift_status_changed(
        assigned_shift["id"], result["history"]["from_status"], result["history"]["to_status"],
        result["history"]["reason"], result["history"]["correlation_id"],
    )
    emit("operations.shift.offer_won", {"shiftId": assigned_shift["id"], "offerId": accepted_offer["id"], "performerId": accepted_offer["performer_id"]})
    emit("operations.shift.performer_assigned", {"shiftId": assigned_shift["id"], "assignmentId": assignment["id"], "performerId": assignment["performer_id"]})
    return {"offer": accepted_offer, "assignment": assignment, "shift": assigned_shift}


@app.post("/public/shift-offers/{token}/decline")
def decline_offer(token: str) -> dict:
    try:
        offer, shift, changed = storage.decline_offer(token_hash(token))
    except KeyError:
        raise HTTPException(status_code=404, detail={"code": "offer_token_not_found"}) from None
    if changed:
        emit("operations.shift.offer_declined", {"shiftId": offer["shift_id"], "offerId": offer["id"], "performerId": offer["performer_id"]})
    return {"offer": offer, "shift": shift}


@app.post("/timesheets")
def create_timesheet(payload: TimesheetCreate) -> dict:
    if storage.get_shift(payload.shift_id) is None:
        raise HTTPException(status_code=404, detail="Shift not found")
    if storage.get_performer(payload.performer_id) is None:
        raise HTTPException(status_code=404, detail="Performer not found")
    item = {"id": str(uuid4()), **payload.model_dump(), "status": "submitted", "created_at": utcnow()}
    item = storage.create_timesheet(item)
    emit("operations.timesheet.submitted", item)
    return item


@app.get("/internal/shifts", response_model=list[InternalShiftRead])
def list_internal_shifts(
    starts_at_from: datetime = Query(alias="startsAtFrom"),
    starts_at_to: datetime = Query(alias="startsAtTo"),
) -> list[dict]:
    starts_at_from = as_aware(starts_at_from).astimezone(timezone.utc)
    starts_at_to = as_aware(starts_at_to).astimezone(timezone.utc)
    if starts_at_to < starts_at_from:
        logger.warning("Invalid internal shifts period", extra={"startsAtFrom": utc_iso(starts_at_from), "startsAtTo": utc_iso(starts_at_to), "error_code": "invalid_period"})
        raise HTTPException(status_code=422, detail={"code": "invalid_period", "message": "startsAtTo must be greater than or equal to startsAtFrom"})
    if starts_at_to - starts_at_from > timedelta(days=366):
        logger.warning("Internal shifts period is too large", extra={"startsAtFrom": utc_iso(starts_at_from), "startsAtTo": utc_iso(starts_at_to), "error_code": "period_too_large"})
        raise HTTPException(status_code=422, detail={"code": "period_too_large", "message": "Period must not exceed 366 calendar days"})

    result = storage.list_shifts(starts_at_from, starts_at_to)
    logger.info("Listed internal shifts", extra={"startsAtFrom": utc_iso(starts_at_from), "startsAtTo": utc_iso(starts_at_to), "count": len(result)})
    return result


@app.post("/internal/shifts/auto-close-unfilled")
def auto_close_unfilled_shifts() -> dict:
    closed, events = storage.auto_close_unfilled(utcnow())
    for event in events:
        shift = event["shift"]
        history = event["history"]
        emit_shift_status_changed(shift["id"], history["from_status"], history["to_status"], history["reason"], history["correlation_id"])
        emit("operations.shift.closed", {"shiftId": shift["id"], "closeReason": shift["close_reason"], "failureReason": shift["failure_reason"], "autoClosed": True})
    return {"closed": closed, "count": len(closed)}


@app.get("/operations/summary")
def summary() -> dict:
    return storage.summary()
