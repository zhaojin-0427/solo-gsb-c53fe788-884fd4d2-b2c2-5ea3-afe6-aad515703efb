"""Consumer bindings and drift-impact reports.

A *consumer binding* declares that a downstream consumer reads a producer
subject as of a chosen **baseline version** with a fixed **reader
contract**. Bindings are validated and stored against one history snapshot
(the baseline version must exist and the reader contract must be able to
read data written under it) and are immutable afterwards.

Binding identity is content-addressed: the binding id hashes the
idempotency key, the producer subject, the baseline version (with its
fingerprint) and the canonical reader contract. The client-supplied
**idempotency key** is unique on its own:

* replaying the same key with the same content returns the stored binding
  (HTTP 200, ``created: false``) — even after the producer advanced;
* replaying the same key with **different** content conflicts (HTTP 409);
* different keys always create distinct bindings.

The producer is referenced by name, never by foreign key, so deleting or
recreating the subject cannot rewrite or cascade into stored bindings or
their reports.

*Drift impact* is evaluated lazily by ``GET .../impact``: the binding's
reader contract is **replayed in version order** against every producer
version after the baseline (reader = binding contract, writer = producer
version). Each failing version is reported with the minimal violation
(shortest field path, lexicographic tie-break — the same rule as
registration), alongside the **first impacted version**. A binding whose
replay finds any failing version flips to status ``impacted`` (sticky:
history is append-only, so a past failure can never un-happen).

Every evaluation that observes a moved producer snapshot is stored as a
**new immutable report revision**; an unchanged snapshot reuses the latest
revision (``created: false``). Revisions are assigned monotonically per
binding and guarded by a unique ``(binding_id, revision)`` constraint —
the compare-and-swap that makes two concurrent generations converge on
one stored revision instead of overwriting each other. Callers may pass
``expected_revision`` (the revision they last observed, ``0`` for none) as
an optimistic CAS token: a mismatch fails with 409 and stores nothing.

Deleting or modifying the producer subject never rewrites stored reports:
once the bound baseline can no longer be resolved (subject gone, or
history replaced so the baseline fingerprint changed), no new revision is
generated — the latest stored revision is served as the historical record,
or 409 when the binding has none.
"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .contract import (
    canonical_document,
    check_schema,
    fingerprint,
    parse_schema,
    render_path,
    select_violation,
)
from .events import CONSUMER_BINDING_CREATED, IMPACT_REPORT_CREATED, emit_event
from .models import ConsumerBinding, ImpactReport, Subject, Version

ACTIVE = "active"
IMPACTED = "impacted"

IDEMPOTENCY_CONFLICT_MESSAGE = (
    "idempotency key was already used with a different binding request"
)
BASELINE_INCOMPATIBLE_MESSAGE = (
    "reader contract cannot read data written under the producer baseline "
    "version"
)
REVISION_CONFLICT_MESSAGE = (
    "expected_revision does not match the binding's latest report revision; "
    "re-read the current impact report and retry"
)
PRODUCER_GONE_MESSAGE = (
    "producer subject no longer carries the bound baseline version and the "
    "binding has no stored impact report"
)


class BindingConflictError(Exception):
    """A 409-style failure carrying a structured ``detail`` payload."""

    def __init__(self, detail: dict) -> None:
        super().__init__(detail["message"])
        self.detail = detail


class BindingSubjectMissing(Exception):
    """The producer subject named by the binding request does not exist."""


class BindingBaselineMissing(Exception):
    """The requested baseline version does not exist on the producer."""


# ---------------------------------------------------------------------------
# binding creation
# ---------------------------------------------------------------------------


def _binding_id(
    idempotency_key: str,
    producer_subject: str,
    baseline_version: int,
    baseline_fingerprint: str,
    reader_canonical: str,
) -> str:
    """Content hash of a binding: the same submission, the same binding."""
    doc = json.dumps(
        {
            "kind": "consumer-binding",
            "idempotency_key": idempotency_key,
            "producer_subject": producer_subject,
            "baseline_version": baseline_version,
            "baseline_fingerprint": baseline_fingerprint,
            "reader": reader_canonical,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return fingerprint(doc)


def _same_submission(
    binding: ConsumerBinding,
    producer_subject: str,
    baseline_version: int,
    reader_fingerprint: str,
) -> bool:
    return (
        binding.producer_subject == producer_subject
        and binding.baseline_version == baseline_version
        and binding.reader_fingerprint == reader_fingerprint
    )


def _idempotency_conflict(existing: ConsumerBinding) -> BindingConflictError:
    return BindingConflictError(
        {
            "message": IDEMPOTENCY_CONFLICT_MESSAGE,
            "kind": "idempotency_conflict",
            "idempotency_key": existing.idempotency_key,
            "binding_id": existing.id,
        }
    )


def create_binding(
    db: Session,
    idempotency_key: str,
    producer_subject: str,
    baseline_version: int,
    reader_content: Any,
) -> tuple[ConsumerBinding, bool]:
    """Validate and store an immutable consumer binding.

    Returns ``(binding, created)``:

    * same idempotency key + same content -> stored binding,
      ``created=False`` (idempotent replay, even after the producer moved);
    * same idempotency key + different content ->
      :class:`BindingConflictError` (nothing is stored);
    * unknown producer subject -> :class:`BindingSubjectMissing`;
    * unknown baseline version -> :class:`BindingBaselineMissing`;
    * invalid reader contract -> :class:`SchemaError`;
    * reader contract incompatible with the baseline version ->
      :class:`BindingConflictError` (nothing is stored).
    """
    schema = parse_schema(reader_content)  # raises SchemaError on invalid input
    reader_canonical = canonical_document(schema)
    reader_fp = fingerprint(reader_canonical)

    existing = (
        db.query(ConsumerBinding)
        .filter(ConsumerBinding.idempotency_key == idempotency_key)
        .first()
    )
    if existing is not None:
        if _same_submission(
            existing, producer_subject, baseline_version, reader_fp
        ):
            return existing, False
        raise _idempotency_conflict(existing)

    subject = db.query(Subject).filter(Subject.name == producer_subject).first()
    if subject is None:
        raise BindingSubjectMissing(f"subject '{producer_subject}' not found")
    history = (
        db.query(Version)
        .filter(Version.subject_id == subject.id)
        .order_by(Version.version)
        .all()
    )
    baseline_row = next(
        (row for row in history if row.version == baseline_version), None
    )
    if baseline_row is None:
        raise BindingBaselineMissing(
            f"baseline version {baseline_version} not found for subject "
            f"'{producer_subject}'"
        )

    # The consumer must be able to read data written under the baseline.
    violations = check_schema(schema, parse_schema(baseline_row.content))
    if violations:
        best = select_violation(violations)
        raise BindingConflictError(
            {
                "message": BASELINE_INCOMPATIBLE_MESSAGE,
                "kind": "baseline_incompatible",
                "errors": [
                    {
                        "version": baseline_row.version,
                        "direction": "backward",
                        "rule": best.rule,
                        "path": list(best.path),
                        "path_string": render_path(best.path),
                        "message": best.message,
                    }
                ],
            }
        )

    binding = ConsumerBinding(
        id=_binding_id(
            idempotency_key,
            producer_subject,
            baseline_version,
            baseline_row.fingerprint,
            reader_canonical,
        ),
        idempotency_key=idempotency_key,
        producer_subject=producer_subject,
        baseline_version=baseline_version,
        baseline_fingerprint=baseline_row.fingerprint,
        bound_latest_version=history[-1].version,
        reader_content=reader_content,
        reader_canonical=reader_canonical,
        reader_fingerprint=reader_fp,
        status=ACTIVE,
    )
    db.add(binding)
    try:
        db.flush()
        emit_event(
            db,
            CONSUMER_BINDING_CREATED,
            subject=producer_subject,
            version=baseline_version,
            fingerprint=binding.id,
            payload={
                "binding_id": binding.id,
                "idempotency_key": idempotency_key,
                "producer_subject": producer_subject,
                "baseline_version": baseline_version,
                "baseline_fingerprint": baseline_row.fingerprint,
                "bound_latest_version": history[-1].version,
                "reader_fingerprint": reader_fp,
            },
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        # Identical submission landed concurrently -> CAS makes it idempotent.
        winner = db.get(ConsumerBinding, binding.id)
        if winner is not None:
            return winner, False
        # Otherwise the idempotency key raced a different submission.
        rival = (
            db.query(ConsumerBinding)
            .filter(ConsumerBinding.idempotency_key == idempotency_key)
            .first()
        )
        if rival is not None:
            raise _idempotency_conflict(rival) from None
        raise  # pragma: no cover - no other constraint can fail
    db.refresh(binding)
    return binding, True


# ---------------------------------------------------------------------------
# drift-impact evaluation
# ---------------------------------------------------------------------------


def _replay_reader(
    binding: ConsumerBinding, versions: list[Version]
) -> list[dict]:
    """Replay the binding's reader contract against ``versions`` in order.

    For every producer version the reader contract is checked as the reader
    of data written under that version; each failing version is reported
    once with the minimal violation (shortest field path, lexicographic
    tie-break). ``versions`` must be ordered by version number.
    """
    reader = parse_schema(binding.reader_content)
    affected: list[dict] = []
    for row in versions:
        violations = check_schema(reader, parse_schema(row.content))
        if not violations:
            continue
        best = select_violation(violations)
        affected.append(
            {
                "version": row.version,
                "fingerprint": row.fingerprint,
                "rule": best.rule,
                "path": list(best.path),
                "path_string": render_path(best.path),
                "message": best.message,
            }
        )
    return affected


def _latest_report(db: Session, binding_id: str) -> ImpactReport | None:
    return (
        db.query(ImpactReport)
        .filter(ImpactReport.binding_id == binding_id)
        .order_by(ImpactReport.revision.desc())
        .first()
    )


def _report_count(db: Session, binding_id: str) -> int:
    return (
        db.query(ImpactReport).filter(ImpactReport.binding_id == binding_id).count()
    )


def _resolve_bound_history(
    db: Session, binding: ConsumerBinding
) -> list[Version] | None:
    """The producer history if the binding's baseline anchor still holds.

    Returns ``None`` when the producer subject is gone or its history was
    replaced (the bound baseline version no longer carries the bound
    fingerprint) — in that case stored reports stay queryable but no new
    revision may be generated.
    """
    subject = (
        db.query(Subject).filter(Subject.name == binding.producer_subject).first()
    )
    if subject is None:
        return None
    history = (
        db.query(Version)
        .filter(Version.subject_id == subject.id)
        .order_by(Version.version)
        .all()
    )
    baseline_row = next(
        (row for row in history if row.version == binding.baseline_version), None
    )
    if baseline_row is None or baseline_row.fingerprint != binding.baseline_fingerprint:
        return None
    return history


def get_impact(
    db: Session,
    binding: ConsumerBinding,
    expected_revision: int | None = None,
) -> tuple[ImpactReport, bool, int, bool]:
    """Evaluate (or reuse) the drift-impact report of ``binding``.

    Returns ``(report, created, revisions, producer_intact)``:

    * the producer snapshot (latest version + fingerprint) is unchanged
      since the latest stored revision -> that revision, ``created=False``;
    * the snapshot moved -> the reader contract is replayed in version
      order over every version after the baseline and the result is stored
      as a **new immutable revision** (``created=True``); a binding with at
      least one failing version flips to status ``impacted``;
    * ``expected_revision`` is an optimistic CAS token: pass the latest
      revision observed (``0`` for none) to fail with 409 instead of
      generating a revision on top of someone else's;
    * the producer subject is gone or its history no longer carries the
      bound baseline -> the latest stored revision is served unchanged
      (``created=False``, ``producer_intact=False``); a binding without any
      stored revision raises :class:`BindingConflictError` — history is
      never rewritten and no revision is fabricated.

    Concurrent generations converge: the binding row is locked while the
    revision is assigned (PostgreSQL), and the unique
    ``(binding_id, revision)`` constraint decides the race where row locks
    are unavailable (SQLite) — the loser re-reads and returns the winner's
    revision.
    """
    # Lock the binding row and re-read it: on PostgreSQL concurrent impact
    # evaluations of the same binding serialise here, so revision numbers
    # are handed out by one transaction at a time. SQLite ignores the
    # clause and relies on the unique-revision race below.
    binding = (
        db.query(ConsumerBinding)
        .filter(ConsumerBinding.id == binding.id)
        .populate_existing()
        .with_for_update()
        .one()
    )
    latest = _latest_report(db, binding.id)

    if expected_revision is not None:
        current = latest.revision if latest is not None else 0
        if expected_revision != current:
            db.rollback()
            raise BindingConflictError(
                {
                    "message": REVISION_CONFLICT_MESSAGE,
                    "kind": "report_revision_conflict",
                    "binding_id": binding.id,
                    "expected_revision": expected_revision,
                    "actual_revision": current,
                }
            )

    history = _resolve_bound_history(db, binding)
    if history is None:
        # Producer deleted or history replaced: never rewrite history —
        # serve the stored record, or fail if there is none.
        if latest is None:
            db.rollback()
            raise BindingConflictError(
                {
                    "message": PRODUCER_GONE_MESSAGE,
                    "kind": "producer_missing",
                    "binding_id": binding.id,
                    "producer_subject": binding.producer_subject,
                }
            )
        revisions = _report_count(db, binding.id)
        db.rollback()  # release the binding lock; read-only response
        return latest, False, revisions, False

    latest_version = history[-1]
    if (
        latest is not None
        and latest.producer_latest_version == latest_version.version
        and latest.producer_latest_fingerprint == latest_version.fingerprint
    ):
        revisions = _report_count(db, binding.id)
        db.rollback()  # release the binding lock; read-only response
        return latest, False, revisions, True

    after_baseline = [
        row for row in history if row.version > binding.baseline_version
    ]
    affected = _replay_reader(binding, after_baseline)
    result = {
        "checked_versions": [row.version for row in after_baseline],
        "affected_versions": affected,
        "first_impacted_version": affected[0]["version"] if affected else None,
    }
    report = ImpactReport(
        binding_id=binding.id,
        revision=(latest.revision + 1) if latest is not None else 1,
        producer_latest_version=latest_version.version,
        producer_latest_fingerprint=latest_version.fingerprint,
        result=result,
    )
    if affected:
        binding.status = IMPACTED
    db.add(report)
    emit_event(
        db,
        IMPACT_REPORT_CREATED,
        subject=binding.producer_subject,
        version=latest_version.version,
        fingerprint=binding.id,
        payload={
            "binding_id": binding.id,
            "producer_subject": binding.producer_subject,
            "revision": report.revision,
            "producer_latest_version": latest_version.version,
            "producer_latest_fingerprint": latest_version.fingerprint,
            "status": binding.status,
            "result": result,
        },
    )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # A concurrent evaluation stored a revision for this binding first.
        # Converge on the winner: it is the latest revision and covers a
        # snapshot at least as recent as the one computed here.
        winner = _latest_report(db, binding.id)
        if winner is not None:
            return winner, False, winner.revision, True
        raise  # pragma: no cover - the winner cannot vanish
    db.refresh(report)
    return report, True, report.revision, True
