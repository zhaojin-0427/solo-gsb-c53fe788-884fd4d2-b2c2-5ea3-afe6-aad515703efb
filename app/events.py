"""Domain event log: unified emitter, subscriptions and replay.

Every state-changing write path (version registration, compatibility
config change, coordinated-release commit, policy-migration commit,
consumer binding creation, impact-report revision) appends **immutable
events in the same database transaction** as the business write:

* a committed change always has its events — none are lost;
* a rolled-back change leaves no events behind;
* concurrent writers cannot duplicate or drop events: the global
  sequence comes from a singleton counter row incremented with
  ``UPDATE ... RETURNING`` inside the emitting transaction. The row lock
  is held until commit, so sequence numbers are handed out in commit
  order and rolled-back increments are rolled back with their
  transaction (the sequence stays gapless).

Readers paginate by cursor (``after_seq``) and each page is cut off at
``snapshot_seq`` — the committed counter value observed when the page is
read. Because sequence order is commit order, every event with
``seq <= snapshot_seq`` is committed and visible, so a page is a
consistent prefix of the log and repeated reads of the same range return
byte-identical results (events are immutable and their payloads are
stored as canonical JSON). Clients checkpoint the last consumed
sequence per subscription and resume from it later — or reset it to 0
and replay the whole log to rebuild a local index.

Events reference subjects **by name**, never by foreign key: deleting or
recreating a subject preserves its full event history.

Emitter contract for write paths: call :func:`emit_event` **after** the
business writes have been flushed (so lock ordering always puts the
counter row last and cannot deadlock against subject/binding row locks)
and **immediately before** ``db.commit()``.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import update
from sqlalchemy.orm import Session

from .models import Event, EventCounter, EventSubscription

# event types
VERSION_REGISTERED = "version_registered"
COMPATIBILITY_CHANGED = "compatibility_changed"
RELEASE_COMMITTED = "release_committed"
MIGRATION_COMMITTED = "migration_committed"
CONSUMER_BINDING_CREATED = "consumer_binding_created"
IMPACT_REPORT_CREATED = "impact_report_created"

EVENT_TYPES = (
    VERSION_REGISTERED,
    COMPATIBILITY_CHANGED,
    RELEASE_COMMITTED,
    MIGRATION_COMMITTED,
    CONSUMER_BINDING_CREATED,
    IMPACT_REPORT_CREATED,
)

COUNTER_ROW_ID = 1


class CheckpointBeyondSnapshot(Exception):
    """The submitted checkpoint is ahead of the committed event log."""

    def __init__(self, checkpoint: int, snapshot_seq: int) -> None:
        super().__init__(
            "checkpoint is ahead of the committed event log; only events "
            "up to the current snapshot may be acknowledged"
        )
        self.checkpoint = checkpoint
        self.snapshot_seq = snapshot_seq


def canonical_payload(payload: dict[str, Any]) -> str:
    """Normalised payload text: sorted keys, compact separators.

    Stored verbatim and served parsed; the stored canonical form is what
    makes repeated reads of the same event byte-identical.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# sequence allocation / singleton seeding
# ---------------------------------------------------------------------------


def ensure_counter(db: Session) -> None:
    """Insert the singleton counter row when missing (idempotent)."""
    if db.get(EventCounter, COUNTER_ROW_ID) is None:
        db.add(EventCounter(id=COUNTER_ROW_ID, value=0))


def reseed_counter(conn) -> None:
    """Re-insert the singleton counter row on a truncated database.

    Used by the PostgreSQL scratch-lease reset (tests/pg_scratch.py),
    which TRUNCATEs every application table between tests.
    """
    conn.execute(EventCounter.__table__.insert().values(id=COUNTER_ROW_ID, value=0))


def current_seq(db: Session) -> int:
    """The latest committed global sequence (0 when no events exist)."""
    row = db.get(EventCounter, COUNTER_ROW_ID)
    return row.value if row is not None else 0


def _next_seq(db: Session) -> int:
    """Allocate the next global sequence number in the caller's transaction.

    The single counter row is incremented with ``UPDATE ... RETURNING``;
    the row lock is held until the calling transaction commits, so
    sequence numbers are assigned in commit order and a rollback returns
    the number to the pool (no gaps, no duplicates).
    """
    seq = db.execute(
        update(EventCounter)
        .where(EventCounter.id == COUNTER_ROW_ID)
        .values(value=EventCounter.value + 1)
        .returning(EventCounter.value)
    ).scalar_one_or_none()
    if seq is None:
        # Counter row missing (unseeded database): create it lazily. A
        # concurrent creator makes this transaction fail on the primary
        # key, surfaced as an IntegrityError like any other lost race.
        db.add(EventCounter(id=COUNTER_ROW_ID, value=1))
        db.flush()
        return 1
    return seq


# ---------------------------------------------------------------------------
# emission
# ---------------------------------------------------------------------------


def emit_event(
    db: Session,
    type: str,
    *,
    subject: str | None,
    version: int | None,
    fingerprint: str | None,
    payload: dict[str, Any],
) -> None:
    """Append an immutable event inside the caller's transaction.

    Must be called after the business writes (lock ordering: the counter
    row comes last) and immediately before ``db.commit()``; a rollback of
    the surrounding transaction removes the event as well.
    """
    db.add(
        Event(
            seq=_next_seq(db),
            type=type,
            subject=subject,
            version=version,
            fingerprint=fingerprint,
            payload=canonical_payload(payload),
        )
    )


# ---------------------------------------------------------------------------
# subscriptions and replay
# ---------------------------------------------------------------------------


def create_subscription(
    db: Session, subjects: list[str], event_types: list[str]
) -> EventSubscription:
    """Register a replay cursor. Empty filter lists match everything."""
    subscription = EventSubscription(
        id=uuid.uuid4().hex,
        subjects=list(subjects),
        event_types=list(event_types),
        checkpoint=0,
    )
    db.add(subscription)
    db.commit()
    db.refresh(subscription)
    return subscription


def read_events(
    db: Session,
    subscription: EventSubscription,
    *,
    after_seq: int | None,
    limit: int,
    upto_seq: int | None,
) -> dict:
    """Read one cursor page of the subscription's filtered event view.

    The page covers ``after_seq < seq <= snapshot_seq`` where
    ``snapshot_seq`` is the committed sequence observed now (pinned to
    ``upto_seq`` when the client passes one, e.g. to page through a fixed
    snapshot). ``after_seq`` defaults to the stored checkpoint, which is
    what makes a sync resumable. Events are immutable, so re-reading the
    same range returns byte-identical pages.
    """
    snapshot = current_seq(db)
    cutoff = min(upto_seq, snapshot) if upto_seq is not None else snapshot
    start = subscription.checkpoint if after_seq is None else after_seq
    query = db.query(Event).filter(Event.seq > start, Event.seq <= cutoff)
    if subscription.subjects:
        query = query.filter(Event.subject.in_(subscription.subjects))
    if subscription.event_types:
        query = query.filter(Event.type.in_(subscription.event_types))
    rows = query.order_by(Event.seq).limit(limit + 1).all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "after_seq": start,
        "snapshot_seq": cutoff,
        "events": rows,
        "next_after_seq": rows[-1].seq if rows else start,
        "has_more": has_more,
    }


def set_checkpoint(
    db: Session, subscription: EventSubscription, seq: int
) -> EventSubscription:
    """Store the client's replay position (``0`` rewinds for a rebuild).

    The checkpoint may not exceed the committed snapshot: a client can
    only acknowledge events that actually exist.
    """
    snapshot = current_seq(db)
    if seq > snapshot:
        raise CheckpointBeyondSnapshot(seq, snapshot)
    subscription.checkpoint = seq
    db.commit()
    db.refresh(subscription)
    return subscription
