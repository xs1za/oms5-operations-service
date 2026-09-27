from datetime import date, datetime, time, timezone
from enum import StrEnum
from hashlib import sha256
from secrets import token_urlsafe
from threading import Lock
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from app.healthcheck.router import router as healthcheck_router
from app.kafka import publish_event
from app.settings import settings

app = FastAPI(title="OMS5 Operations Service", version="0.2.0", root_path=settings.root_path)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8088", "http://127.0.0.1:8088"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(healthcheck_router)

clients: dict[str, dict] = {}
performers: dict[str, dict] = {}
performers_by_employee_id: dict[int, str] = {}
shifts: dict[str, dict] = {}
shift_status_history: list[dict] = []
offer_campaigns: dict[str, dict] = {}
shift_offers: dict[str, dict] = {}
offer_views: list[dict] = []
assignments: dict[str, dict] = {}
timesheets: dict[str, dict] = {}
state_lock = Lock()


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


class OfferCampaignCreate(BaseModel):
    performer_ids: list[str] = Field(min_length=1)
    created_by_user_id: str | None = None


class TimesheetCreate(BaseModel):
    performer_id: str
    shift_id: str
    work_date: date
    hours: float = Field(gt=0, le=24)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def record_status_change(shift: dict, new_status: ShiftStatus, reason: str | None = None, actor_user_id: str | None = None) -> dict:
    previous_status = shift["status"]
    if new_status not in ALLOWED_TRANSITIONS[ShiftStatus(previous_status)]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "invalid_shift_transition", "from": previous_status, "to": new_status},
        )
    shift["status"] = new_status
    shift["updated_at"] = utcnow()
    history_item = {
        "id": str(uuid4()),
        "shift_id": shift["id"],
        "from_status": previous_status,
        "to_status": new_status,
        "transition_code": f"{previous_status}->{new_status}",
        "reason": reason,
        "actor_user_id": actor_user_id,
        "occurred_at": utcnow(),
        "correlation_id": f"corr_{uuid4().hex}",
        "metadata": {},
    }
    shift_status_history.append(history_item)
    emit(
        "operations.shift.status_changed",
        {"shiftId": shift["id"], "previousStatus": previous_status, "newStatus": new_status, "reason": reason},
        history_item["correlation_id"],
        {"userId": actor_user_id} if actor_user_id else None,
    )
    return history_item


def get_offer_by_token(token: str) -> dict:
    hashed = token_hash(token)
    for offer in shift_offers.values():
        if offer["token_hash"] == hashed:
            return offer
    raise HTTPException(status_code=404, detail={"code": "offer_token_not_found"})


def offer_availability(offer: dict) -> dict:
    shift = shifts[offer["shift_id"]]
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
    clients[item["id"]] = item
    emit("operations.client_created", item)
    return item


@app.post("/performers/sync")
def sync_performer(payload: PerformerSync) -> dict:
    performer_id = performers_by_employee_id.get(payload.employee_id, str(uuid4()))
    item = {
        "id": performer_id,
        **payload.model_dump(),
        "synced_at": utcnow(),
    }
    created = performer_id not in performers
    performers[performer_id] = item
    performers_by_employee_id[payload.employee_id] = performer_id
    emit("operations.performer.created" if created else "operations.performer.updated", item)
    return item


@app.post("/shifts")
def create_shift(payload: ShiftCreate) -> dict:
    if payload.client_id not in clients:
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
    shifts[item["id"]] = item
    emit("operations.shift.created", item)
    return item


@app.post("/shifts/{shift_id}/transition")
def transition_shift(shift_id: str, payload: ShiftTransition) -> dict:
    if shift_id not in shifts:
        raise HTTPException(status_code=404, detail="Shift not found")
    shift = shifts[shift_id]
    if payload.new_status == ShiftStatus.CLOSED:
        if not payload.close_reason:
            raise HTTPException(status_code=422, detail="close_reason is required when closing shift")
        shift["close_reason"] = payload.close_reason
        shift["failure_reason"] = payload.failure_reason
    history_item = record_status_change(shift, payload.new_status, payload.reason, payload.actor_user_id)
    if payload.new_status == ShiftStatus.CLOSED:
        emit("operations.shift.closed", {"shiftId": shift_id, "closeReason": shift["close_reason"], "failureReason": shift["failure_reason"]})
    return {"shift": shift, "history": history_item}


@app.get("/shifts/{shift_id}/history")
def get_shift_history(shift_id: str) -> list[dict]:
    if shift_id not in shifts:
        raise HTTPException(status_code=404, detail="Shift not found")
    return [item for item in shift_status_history if item["shift_id"] == shift_id]


@app.post("/shifts/{shift_id}/offer-campaigns")
def create_offer_campaign(shift_id: str, payload: OfferCampaignCreate) -> dict:
    if shift_id not in shifts:
        raise HTTPException(status_code=404, detail="Shift not found")
    shift = shifts[shift_id]
    if shift["status"] != ShiftStatus.SOURCING:
        raise HTTPException(status_code=409, detail={"code": "shift_not_sourcing"})
    missing = [performer_id for performer_id in payload.performer_ids if performer_id not in performers]
    if missing:
        raise HTTPException(status_code=404, detail={"code": "performers_not_found", "performerIds": missing})

    campaign = {
        "id": str(uuid4()),
        "shift_id": shift_id,
        "status": CampaignStatus.SENT,
        "created_by_user_id": payload.created_by_user_id,
        "created_at": utcnow(),
    }
    offer_campaigns[campaign["id"]] = campaign
    created_offers = []
    for performer_id in payload.performer_ids:
        raw_token = token_urlsafe(32)
        offer = {
            "id": str(uuid4()),
            "campaign_id": campaign["id"],
            "shift_id": shift_id,
            "performer_id": performer_id,
            "status": OfferStatus.SENT,
            "token_hash": token_hash(raw_token),
            "expires_at": shift["starts_at"],
            "sent_at": utcnow(),
            "responded_at": None,
        }
        shift_offers[offer["id"]] = offer
        offer_for_response = offer | {"public_url": public_offer_url(raw_token), "token": raw_token}
        created_offers.append(offer_for_response)
        emit("operations.shift.offer_sent", {"shiftId": shift_id, "offerId": offer["id"], "performerId": performer_id})
    emit("operations.shift.offer_campaign_created", {"shiftId": shift_id, "campaignId": campaign["id"]})
    return {"campaign": campaign, "offers": created_offers}


@app.get("/public/shift-offers/{token}")
def get_public_offer(token: str) -> dict:
    offer = get_offer_by_token(token)
    shift = shifts[offer["shift_id"]]
    performer = performers[offer["performer_id"]]
    return {"offer": offer, "shift": shift, "performer": performer, **offer_availability(offer)}


@app.post("/public/shift-offers/{token}/viewed")
def mark_offer_viewed(token: str) -> dict:
    offer = get_offer_by_token(token)
    view = {
        "id": str(uuid4()),
        "offer_id": offer["id"],
        "viewed_at": utcnow(),
        "ip_address": None,
        "user_agent": None,
        "correlation_id": f"corr_{uuid4().hex}",
    }
    offer_views.append(view)
    if offer["status"] == OfferStatus.SENT:
        offer["status"] = OfferStatus.VIEWED
    availability = offer_availability(offer)
    emit("operations.shift.offer_viewed", {"shiftId": offer["shift_id"], "offerId": offer["id"], **availability}, view["correlation_id"])
    return {"status": "logged", "offerStatus": offer["status"], "shiftStatus": shifts[offer["shift_id"]]["status"], **availability}


@app.post("/public/shift-offers/{token}/accept")
def accept_offer(token: str) -> dict:
    with state_lock:
        offer = get_offer_by_token(token)
        shift = shifts[offer["shift_id"]]
        availability = offer_availability(offer)
        if not availability["available"]:
            if availability["reason"] == "shift_already_assigned":
                offer["status"] = OfferStatus.LOST
                raise HTTPException(status_code=409, detail={"code": "offer_lost", "message": "Shift already assigned"})
            raise HTTPException(status_code=409, detail={"code": availability["reason"]})

        offer["status"] = OfferStatus.WON
        offer["responded_at"] = utcnow()
        assignment = {
            "id": str(uuid4()),
            "shift_id": shift["id"],
            "performer_id": offer["performer_id"],
            "offer_id": offer["id"],
            "status": "assigned",
            "assigned_at": utcnow(),
            "removed_at": None,
            "reason": None,
        }
        assignments[assignment["id"]] = assignment
        shift["assigned_performer_id"] = offer["performer_id"]
        for other_offer in shift_offers.values():
            if other_offer["shift_id"] == shift["id"] and other_offer["id"] != offer["id"] and other_offer["status"] in {OfferStatus.SENT, OfferStatus.VIEWED}:
                other_offer["status"] = OfferStatus.LOST
                emit("operations.shift.offer_lost", {"shiftId": shift["id"], "offerId": other_offer["id"], "performerId": other_offer["performer_id"]})
        record_status_change(shift, ShiftStatus.CHOICE, "first accepted wins")

    emit("operations.shift.offer_won", {"shiftId": shift["id"], "offerId": offer["id"], "performerId": offer["performer_id"]})
    emit("operations.shift.performer_assigned", {"shiftId": shift["id"], "assignmentId": assignment["id"], "performerId": offer["performer_id"]})
    return {"offer": offer, "assignment": assignment, "shift": shift}


@app.post("/public/shift-offers/{token}/decline")
def decline_offer(token: str) -> dict:
    offer = get_offer_by_token(token)
    if offer["status"] not in {OfferStatus.WON, OfferStatus.LOST, OfferStatus.EXPIRED, OfferStatus.CANCELLED}:
        offer["status"] = OfferStatus.DECLINED
        offer["responded_at"] = utcnow()
        emit("operations.shift.offer_declined", {"shiftId": offer["shift_id"], "offerId": offer["id"], "performerId": offer["performer_id"]})
    return {"offer": offer, "shift": shifts[offer["shift_id"]]}


@app.post("/timesheets")
def create_timesheet(payload: TimesheetCreate) -> dict:
    if payload.shift_id not in shifts:
        raise HTTPException(status_code=404, detail="Shift not found")
    if payload.performer_id not in performers:
        raise HTTPException(status_code=404, detail="Performer not found")
    item = {"id": str(uuid4()), **payload.model_dump(), "status": "submitted", "created_at": utcnow()}
    timesheets[item["id"]] = item
    emit("operations.timesheet.submitted", item)
    return item


@app.post("/internal/shifts/auto-close-unfilled")
def auto_close_unfilled_shifts() -> dict:
    closed = []
    for shift in shifts.values():
        if shift["status"] != ShiftStatus.SOURCING or shift.get("assigned_performer_id"):
            continue
        if utcnow() <= reporting_period_close_datetime(shift["starts_at"]):
            continue
        shift["close_reason"] = CloseReason.FAILED
        shift["failure_reason"] = FailureReason.NO_PERFORMER_FOUND
        shift["auto_closed"] = True
        shift["auto_close_reason"] = "reporting_period_expired"
        record_status_change(shift, ShiftStatus.CLOSED, "reporting_period_expired")
        emit("operations.shift.closed", {"shiftId": shift["id"], "closeReason": shift["close_reason"], "failureReason": shift["failure_reason"], "autoClosed": True})
        closed.append(shift["id"])
    return {"closed": closed, "count": len(closed)}


@app.get("/operations/summary")
def summary() -> dict:
    return {
        "clients": len(clients),
        "performers": len(performers),
        "shifts": len(shifts),
        "offerCampaigns": len(offer_campaigns),
        "offers": len(shift_offers),
        "assignments": len(assignments),
        "timesheets": len(timesheets),
    }
