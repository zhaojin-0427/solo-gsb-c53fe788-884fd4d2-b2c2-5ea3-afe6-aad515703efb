"""Event subscription and replay endpoints."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..events import (
    CheckpointBeyondSnapshot,
    create_subscription,
    read_events,
    set_checkpoint,
)
from ..models import Event, EventSubscription
from ..schemas import (
    CheckpointUpdate,
    EventOut,
    EventPage,
    EventSubscriptionCreate,
    EventSubscriptionOut,
)

router = APIRouter(prefix="/event-subscriptions", tags=["event-subscriptions"])


def get_subscription_or_404(db: Session, subscription_id: str) -> EventSubscription:
    subscription = db.get(EventSubscription, subscription_id)
    if subscription is None:
        raise HTTPException(
            status_code=404,
            detail=f"event subscription '{subscription_id}' not found",
        )
    return subscription


def _subscription_out(subscription: EventSubscription) -> EventSubscriptionOut:
    return EventSubscriptionOut(
        subscription_id=subscription.id,
        subjects=list(subscription.subjects),
        event_types=list(subscription.event_types),
        checkpoint=subscription.checkpoint,
        created_at=subscription.created_at,
    )


def _event_out(event: Event) -> EventOut:
    return EventOut(
        seq=event.seq,
        type=event.type,
        subject=event.subject,
        version=event.version,
        fingerprint=event.fingerprint,
        payload=json.loads(event.payload),
        recorded_at=event.recorded_at,
    )


@router.post("", status_code=201, response_model=EventSubscriptionOut)
def create(body: EventSubscriptionCreate, db: Session = Depends(get_db)):
    subscription = create_subscription(
        db,
        subjects=body.subjects,
        event_types=[event_type.value for event_type in body.event_types],
    )
    return _subscription_out(subscription)


@router.get("/{subscription_id}", response_model=EventSubscriptionOut)
def get_one(subscription_id: str, db: Session = Depends(get_db)):
    return _subscription_out(get_subscription_or_404(db, subscription_id))


@router.get("/{subscription_id}/events", response_model=EventPage)
def events(
    subscription_id: str,
    after_seq: int | None = Query(
        default=None,
        ge=0,
        description=(
            "Cursor: read events with seq > after_seq. Defaults to the "
            "subscription's stored checkpoint (resumable sync)."
        ),
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    upto_seq: int | None = Query(
        default=None,
        ge=0,
        description=(
            "Pin the snapshot cutoff (e.g. the snapshot_seq of the first "
            "page) to page through a fixed snapshot byte-identically."
        ),
    ),
    db: Session = Depends(get_db),
):
    subscription = get_subscription_or_404(db, subscription_id)
    page = read_events(
        db, subscription, after_seq=after_seq, limit=limit, upto_seq=upto_seq
    )
    return EventPage(
        subscription_id=subscription.id,
        after_seq=page["after_seq"],
        snapshot_seq=page["snapshot_seq"],
        events=[_event_out(event) for event in page["events"]],
        next_after_seq=page["next_after_seq"],
        has_more=page["has_more"],
    )


@router.post("/{subscription_id}/checkpoint", response_model=EventSubscriptionOut)
def checkpoint(
    subscription_id: str, body: CheckpointUpdate, db: Session = Depends(get_db)
):
    subscription = get_subscription_or_404(db, subscription_id)
    try:
        subscription = set_checkpoint(db, subscription, body.seq)
    except CheckpointBeyondSnapshot as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "kind": "checkpoint_beyond_snapshot",
                "checkpoint": exc.checkpoint,
                "snapshot_seq": exc.snapshot_seq,
            },
        ) from exc
    return _subscription_out(subscription)
